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
      lowm: :none,
      event_timers: nil,
      max_event_time_timers: [],
      wm_hold: Time.max_timestamp
  end

  # API

  def start_link(parent) do
    GenServer.start_link(__MODULE__, parent)
  end

  def start_linked() do
    start_link(self())
  end

  def set_timer(pid, namespace, id, time, domain) do
    GenServer.call pid, {:set_timer, namespace, id, time, domain}
  end

  def clear_timer(pid, namespace, id, domain) do
    GenServer.call pid, {:clear_timer, namespace, id, domain}
  end

  #def get_timers(time, domain)

  def advance_input_watermark(pid, new_watermark) do
    GenServer.call pid, {:advance_input_watermark, new_watermark}
  end

  def update_data_hold(pid, new_hold) do
    GenServer.call pid, {:update_data_hold, new_hold}
  end

  # Callbacks

  def init(parent) do
    state = %State{
      parent: parent,
      event_timers: PQ.new
    }

    {:ok, state}
  end

  def handle_call({:set_timer, namespace, id, time, domain}, _from, state), do: do_set_timer(namespace, id, time, domain, state)
  def handle_call({:clear_timer, namespace, id, domain}, _from, state), do: do_clear_timer(namespace, id, domain, state)
  def handle_call({:advance_input_watermark, new_watermark}, _from, state), do: do_advance_input_watermark(new_watermark, state)
  def handle_call({:update_data_hold, new_hold}, _from, state), do: do_update_data_hold(new_hold, state)

  # private processing

  defp do_set_timer(namespace, id, Time.max_timestamp, :event_time, state) do
    # TODO!!! check if this ID has already been inserted
    max_event_time_timers = [{namespace, id, Time.max_timestamp, :event_time} | state.max_event_time_timers]
    {:reply, :ok, %{state | max_event_time_timers: max_event_time_timers}}
  end

  defp do_set_timer(namespace, id, time, :event_time, state) do
    # TODO!!! check if this ID has already been inserted
    event_timers = PQ.put state.event_timers, Time.raw(time), {namespace, id, time, :event_time}
    {:reply, :ok, %{state | event_timers: event_timers}}
  end

  defp do_clear_timer(namespace, id, domain, state)

  defp do_clear_timer(namespace, id, :event_time, state) do
    event_timers = PQ.delete state.event_timers,
      fn
        _, {^namespace, ^id, _, :event_time} -> true
        _, _ -> false
      end

    max_event_time_timers = Enum.reject state.max_event_time_timers,
      fn
          {^namespace, ^id, _, :event_time} -> true
          _ -> false
      end

    %{state | event_timers: event_timers, max_event_time_timers: max_event_time_timers}
  end

  # special case for the max_timestamp---want to fire all timers, then all max event time timers, and we know we are finished
  defp do_advance_input_watermark(new_watermark, state) do
    if Time.before?(new_watermark, state.liwm), do: raise "Tried to advance input watermark to time which is before current input watermark"

    # set new internal IWM
    state = %{state | liwm: new_watermark}

    # advance OWM as far as possible given current data holds
    # cast this message to the executor after the timer firings, in case it's a max_timestamp and we want to finish
    {new_owm, state} = calculate_advanced_owm(state)


    # check for any new timers firing because of the time advancement
    # cast these firings to the executor

    {fired_timers, state} =
      case new_watermark do
        Time.max_timestamp ->
          # take all the timers and all max timestamp timers
          {timers, event_timers} = PQ.take_all state.event_timers

          # get vals
          timers = Enum.map timers, fn {_, val} -> val end

          timers = timers ++ state.max_event_time_timers

          {timers, %{state | event_timers: event_timers, max_event_time_timers: []}}

        timestamp ->
          {timers, event_timers} =
            state.event_timers
            |> PQ.take_before(Time.raw(timestamp))

          timers = Enum.map timers, fn {_, val} -> val end

          {timers, %{state | event_timers: event_timers}}
      end

    TX.notify_of_timers(state.parent, fired_timers)
    TX.notify_downstream_of_advanced_owm(state.parent, new_owm)

    # reply back with ok
    {:reply, :ok, state}
  end

  defp do_update_data_hold(new_hold, state) do
    # TODO check for validity of hold wrt watermarks
    # todo clipping?

    # set new internal data hold
    state = %{state | wm_hold: new_hold}

    # advance OWM as far as possible with the new data holds
    # cast this message to the executor
    {new_owm, state} = calculate_advanced_owm(state)
    TX.notify_downstream_of_advanced_owm(state.parent, new_owm)

    {:reply, :ok, state}
  end

  defp calculate_advanced_owm(state) do
    # get that LATER of:
    # - the previous OWM
    # - the EARLIER of
    # -- the current IWM
    # -- the current WM hold

    new_owm = Time.later(state.lowm, Time.earlier(state.liwm, state.wm_hold))

    {new_owm, %{state | lowm: new_owm}}
  end

end
