defmodule Dataflow.DirectRunner.TimingManager do
  @moduledoc """
  Manages watermark positions and timer firings for an executing transform.

  Timers can be set in event time or processing time domains.

  A timer "fires" when the respective watermark or time passes the time of the timer, that is, when the time is _after_
  (not equal to) the time for which the timer is set. An exception is the maximum watermark - when a timer is set for the
  "end of time", then since we cannot go past the end of time, the timer fires when we reach it.
  """

  use GenServer
  use Dataflow.Utils.Time
  alias Dataflow.Utils.PriorityQueue, as: PQ
  alias Dataflow.DirectRunner.TransformExecutor, as: TX

  defmodule State do
    defstruct \
      parent: nil,
      liwm: Time.min_timestamp,
      lowm: Time.min_timestamp,
      desired_lowm: nil,
      manage_own_lowm?: false,
      send_liwm_timers?: false,
      event_timers: nil, # PQ with timer raw time as key
      wm_hold: Time.max_timestamp,
      all_holds: nil # PQ with hold time as key
  end

  # API

  def start_link(parent, opts \\[]) do
    GenServer.start_link(__MODULE__, {parent, opts})
  end

  def start_linked(opts \\ []) do
    start_link(self(), opts)
  end

  def set_timer(pid, namespace, time, domain) do
    GenServer.call pid, {:set_timer, namespace, time, domain}
  end

  def clear_timer(pid, namespace, time, domain) do
    GenServer.call pid, {:clear_timer, namespace, time, domain}
  end

  def clear_timers(pid, namespace, domain) do
    GenServer.call pid, {:clear_timers, namespace, domain}
  end

  def get_liwm(pid) do
    GenServer.call pid, :get_liwm
  end

  def get_lowm(pid) do
    GenServer.call pid, :get_lowm
  end

  def get_wm_hold(pid) do
    GenServer.call pid, :get_wm_hold
  end

  #def get_timers(time, domain)

  def advance_input_watermark(pid, new_watermark) do
    GenServer.call pid, {:advance_input_watermark, new_watermark}
  end

  def update_hold(pid, key, hold) do
    GenServer.call pid, {:update_hold, key, hold}
  end

  def remove_hold(pid, key) do
    GenServer.call pid, {:remove_hold, key}
  end

  # specifically for the case when we are merging windows and want to remove multiple holds at once
  def remove_holds(pid, keys) do
    GenServer.call pid, {:remove_holds, keys}
  end

  def refresh_hold(pid) do
    GenServer.call pid, :refresh_hold
  end


  # for explicit OWM management
  def set_liwm_timers(pid, set?) do
    case set? do
      true -> GenServer.call pid, :set_liwm_timers
      false -> GenServer.call pid, :unset_liwm_timers
    end
  end

  def get_desired_lowm(pid) do
    GenServer.call pid, :get_desired_lowm
  end

  def advance_desired_lowm(pid, dlowm) do
    GenServer.call pid, {:advance_desired_lowm, dlowm}
  end

  # Callbacks

  def init({parent, opts}) do
    state = %State{
      parent: parent,
      event_timers: PQ.new(&Time.compare/2),
      all_holds: PQ.new(&Time.compare/2)
    }

    state =
      if opts[:manage_owm_lowm] do
        state = %{state | manage_owm_lowm?: true, desired_lowm: Time.min_timestamp}
      else
        state
      end

    {:ok, state}
  end

  def handle_call({:set_timer, namespace, time, domain}, _from, state), do: do_set_timer(namespace, time, domain, state)
  def handle_call({:clear_timer, namespace, time, domain}, _from, state), do: do_clear_timer(namespace, time, domain, state)
  def handle_call({:clear_timers, namespace, domain}, _from, state), do: do_clear_timers(namespace, domain, state)

  def handle_call({:advance_input_watermark, new_watermark}, _from, state), do: do_advance_input_watermark(new_watermark, state)
  def handle_call({:update_hold, key, hold}, _from, state), do: do_update_hold(key, hold, state)
  def handle_call({:remove_hold, key}, _from, state), do: do_remove_hold(key, state)
  def handle_call({:remove_holds, keys}, _from, state), do: do_remove_holds(keys, state)
  def handle_call(:refresh_hold, _from, state), do: do_refresh_hold(state)

  def handle_call(:get_liwm, _from, state), do: {:reply, state.liwm, state}
  def handle_call(:get_lowm, _from, state), do: {:reply, state.lowm, state}
  def handle_call(:get_wm_hold, _from, state), do: {:reply, state.wm_hold, state}

  # callbacks for WM partition transforms
  def handle_call(:get_desired_lowm, _from, %{manage_own_lowm?: false}), do: raise "This transform does not cause a WM domain partition."
  def handle_call(:get_desired_lowm, _from, state), do: {:reply, state.desired_lowm, state}

  def handle_call({:advance_desired_lowm, _}, _from, %{manage_own_lowm?: false}), do: raise "This transform does not cause a WM domain partition."
  def handle_call({:advance_desired_lowm, dlowm}, _from, state), do: do_advance_desired_lowm(dlowm, state)


  # private processing

  defp do_set_timer(namespace, time, :event_time, state) do
    # todo do we fire a timer now if the current time is past the timer time?
    event_timers = PQ.put_unique state.event_timers, time, {namespace, time, :event_time}
    {:reply, :ok, %{state | event_timers: event_timers}}
  end

  defp do_clear_timer(namespace, time, domain, state)

  defp do_clear_timer(namespace, time, :event_time, state) do
    event_timers = PQ.delete state.event_timers,
      fn
        ^time, {^namespace, ^time, :event_time} -> true
        _, _ -> false
      end

    {:reply, :ok, %{state | event_timers: event_timers}}
  end

  defp do_clear_timers(namespace, :event_time, state) do
    event_timers = PQ.delete state.event_timers,
      fn
        _, {^namespace, _, :event_time} -> true
        _, _ -> false
      end

    {:reply, :ok, %{state | event_timers: event_timers}}
  end

  # special case for the max_timestamp---want to fire all timers, then all max event time timers, and we know we are finished
  defp do_advance_input_watermark(new_watermark, state) do
    if Time.before?(new_watermark, state.liwm), do: raise "Tried to advance input watermark to time which is before current input watermark. IWMs must be monotonic."

    # set new internal IWM
    state = %{state | liwm: new_watermark}

    # advance OWM as far as possible given current data holds
    # cast this message to the executor after the timer firings, in case it's a max_timestamp and we want to finish
    old_owm = state.lowm
    {new_owm, state} = advance_owm(state)


    # check for any new timers firing because of the time advancement
    # cast these firings to the executor

    # first, check if the transform wants timers on any liwm advancement
    fired_timers =
      if state.send_liwm_timers? do
        [{:liwm_update, new_watermark, :event_time}]
      else
        []
      end

    {timers, state} =
      case new_watermark do
        Time.max_timestamp ->
          # take all the timers
          {timers, event_timers} = PQ.take_all state.event_timers

          {timers, %{state | event_timers: event_timers}}

        timestamp ->
          {timers, event_timers} =
            state.event_timers
            |> PQ.take_before(Time.raw(timestamp))

          {timers, %{state | event_timers: event_timers}}
      end

    fired_timers = fired_timers ++ Enum.map(timers, fn {_, val} -> val end)

    unless Enum.empty?(fired_timers), do: TX.notify_of_timers(state.parent, fired_timers)
    unless old_owm == new_owm, do: TX.notify_downstream_of_advanced_owm(state.parent, new_owm)

    # reply back with ok
    {:reply, :ok, state}
  end

  defp do_update_hold(key, :none, state), do: do_remove_hold(key, state)

  defp do_update_hold(key, hold, state) do
    # TODO this is really really inefficient.

    # first remove the hold with this key (but actually traverse the whole list - see above.)
    all_holds =
      PQ.delete state.all_holds, fn
        _time, {^key, _hold} -> true
        _, _ -> false
      end

    # now insert the new hold
    all_holds = PQ.put(all_holds, hold, {key, hold})

    {:reply, :ok, %{state | all_holds: all_holds}}
  end

  defp do_remove_hold(key, state) do
    all_holds =
      PQ.delete state.all_holds, fn
        _time, {^key, _hold} -> true
        _, _ -> false
      end

    {:reply, :ok, %{state | all_holds: all_holds}}
  end

  defp do_remove_holds(keys, state) do
    all_holds =
      PQ.delete state.all_holds, fn
        _time, {key, _hold} -> key in keys
      end

    {:reply, :ok, %{state | all_holds: all_holds}}
  end

  defp do_refresh_hold(state) do
    # TODO check for validity of hold wrt watermarks
    # todo clipping?

    new_hold =
      if PQ.empty? state.all_holds do
        Time.max_timestamp
      else
        {_time, {_key, hold}} = PQ.peek state.all_holds
        hold
      end

    # set new internal data hold
    state = %{state | wm_hold: new_hold}

    # advance OWM as far as possible with the new data holds
    # cast this message to the executor if necessary
    old_owm = state.lowm
    {new_owm, state} = advance_owm(state)

    unless old_owm == new_owm, do: TX.notify_downstream_of_advanced_owm(state.parent, new_owm)

    {:reply, :ok, state}
  end

  defp advance_owm(state) do
    # get that LATER of:
    # - the previous OWM
    # - the EARLIER of
    # -- the current IWM _OR_ the desired OWM
    # -- the current WM hold

    base_wm =
      case state.manage_own_lowm? do
        true -> state.desired_lowm
        false -> state.liwm
      end

    new_owm = Time.later(state.lowm, Time.earlier(base_wm, state.wm_hold))

    {new_owm, %{state | lowm: new_owm}}
  end

  defp do_advance_desired_lowm(new_dlowm, state) do
    if Time.before?(new_dlowm, state.lowm), do: raise "Tried to advance desired output watermark to time which is before current output watermark. OWMs must be monotonic."

    # set the new DLOWM
    state = %{state | desired_lowm: new_dlowm}

    # advance OWM as far as possible given current data holds
    old_owm = state.lowm
    {new_owm, state} = advance_owm(state)

    unless old_owm == new_owm, do: TX.notify_downstream_of_advanced_owm(state.parent, new_owm)

    # reply back with ok
    {:reply, :ok, state}
  end

end
