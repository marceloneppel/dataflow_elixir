defmodule Dataflow.DirectRunner.ReducingEvaluator do
  use Dataflow.DirectRunner.TransformEvaluator, type: :reducing

  alias Dataflow.{Window, Utils.Time, PValue, Window.WindowingStrategy, Trigger}
  alias Dataflow.DirectRunner.ReducingEvaluator.{PaneInfoTracker, TriggerDriver, WatermarkHoldManager}
  alias Dataflow.Window.WindowFn.Callable, as: WindowFnP
  alias Dataflow.Window.OutputTimeFn
  alias Dataflow.Window.PaneInfo
  require Time
  require Logger

  defmodule Reducer do
    @type t :: module
    @type state :: any
    @type timer_op :: {:set, Time.timestamp, Time.domain} | {:cancel, Time.timestamp, Time.domain}

    @type state_with_timers :: state | {state, [timer_op]}
    @type context :: {key :: any, Window.t, WindowingStrategy.t, timers :: [any]}

    @callback init(transform) :: state_with_timers when transform: Dataflow.PTransform.t
    @callback process_value(value, context, state) :: state_with_timers when value: any
    @callback merge_accumulators([state], context) :: state_with_timers
    @callback emit(pane_info, context, state) :: {[element], state_with_timers} when element: any, pane_info: any
    @callback clear_accumulator(state) :: state

    def module_for(%Dataflow.Transforms.Core.GroupByKey{}), do: Dataflow.DirectRunner.Reducers.GBKReducer

    def module_for(%{__struct__: module}), do: raise "No reducer available for transform #{module}"
  end

  defmodule State do
    @type key :: any
    @type value :: any
    @type window_info :: {
      hold :: any,
      trigger :: any,
      new_elements? :: boolean,
      last_pane :: PaneInfo.t | :none,
      elements :: %{
        optional(key) => Reducer.state
      }
    } | :closed

    @type t :: %__MODULE__{
      windows: %{optional(Window.t) => window_info},
      windowing: WindowingStrategy.t,
      reducer: Reducer.t,
      trigger_driver: TriggerDriver.t,
      trigger: Trigger.t,
      merging?: boolean,
      transform: Dataflow.PTransform.t,
      liwm: Time.timestamp,
      lowm: Time.timestamp | :none,
      watermark_state: WatermarkHoldManager.state
    }

    defstruct \
    windows: %{},
    windowing: nil,
    reducer: nil,
    trigger_driver: nil,
    trigger: nil,
    merging?: true,
    transform: nil,
    liwm: Time.min_timestamp(),
    lowm: :none,
    watermark_state: nil
  end

  def init(transform, input) do
    {:ok,
      :no_update,
      %State{
        windowing: input.windowing_strategy,
        reducer: Reducer.module_for(transform),
        merging?: not WindowFnP.non_merging?(input.windowing_strategy.window_fn),
        transform: transform,
        trigger_driver: TriggerDriver.module_for(input.windowing_strategy.trigger),
        trigger: input.windowing_strategy_trigger
      }
     }
  end

  def transform_elements(elements, state) do
    {mapping, state} = maybe_merge_windows(elements, state)

    state = state |> process_elements(elements, mapping)

    # now check for trigger firings

    {[], state}
  end

  def update_input_watermark(watermark, state) do
    # TODO
    {[], state}
  end

  def finish(_state) do
    :ok # todo all elements that should have been emitted already were?
  end

  defp maybe_merge_windows(_, %State{merging?: false} = state), do: {%{}, state}
  defp maybe_merge_windows(elements, state) do
    windowing = state.windowing

    # get any new windows which are present in the elements, and merge the whole set, building a LUT
    active_windows = for {window, info} when info != :closed <- state.windows, into: %MapSet{}, do: window
    element_windows = Enum.flat_map(elements, fn {_kv, _timestamp, windows} -> windows end) |> Enum.into(%MapSet{})
    all_windows = MapSet.union active_windows, element_windows

    # obtain the merge result TODO: check for transitive merging
    merge_result = WindowFnP.merge(windowing.window_fn, MapSet.to_list(all_windows))

    # build the map for new additions
    merge_map = build_merge_map merge_result

    # filter the merge result to retain active windows only (those with existing state)
    active_merge_result =
      Enum.reduce merge_result, [], fn {to_merge, result}, acc ->
        case Enum.filter to_merge, &MapSet.member?(active_windows, &1) do
          [] -> acc
          to_merge -> [{to_merge, result} | acc]
        end
      end

    # merge active window state
    {merge_map, merge_window_state(state, active_merge_result)}
  end

  defp build_merge_map(merge_result) do
    Enum.reduce merge_result, %{}, fn {to_merge, result}, map ->
      new_mappings = for from <- to_merge, into: %{}, do: {from, result}
      Map.merge map, new_mappings, fn window, _, _ -> raise "A WindowFn tried to merge #{inspect window} into two different resultant windows." end
    end
  end

  defp merge_window_state(state, merge_result) do
    Enum.reduce merge_result, state, &merge_windows/2
  end

  defp merge_windows({[window_to_merge], result_window}, state) do
    # only one window to merge, so just do a replacement

    # get and remove state of window_to_merge
    {window_state, windows} = Map.get_and_update! state.windows, window_to_merge, fn _ -> :pop end

    {hold_state, trigger_state, new_elements_state, _last_pane_state, reducer_state} = window_state

    hold_state = WatermarkHoldManager.merge([hold_state], hold_state, result_window, state.liwm, state.lowm, state.windowing_strategy) # possibly need to recalculate holds in new window

    # todo need to recalculate trigger state maybe?

    new_window_state = {hold_state, trigger_state, new_elements_state, :none, reducer_state} # discard pane state

    windows = Map.put windows, result_window, new_window_state

    %{state | windows: windows}
  end

  defp merge_windows({windows_to_merge, result_window}, state) do
    # todo check invariants

    # get window states, and remove them from the map at the same time
    {window_states, windows} =
      Enum.map_reduce windows_to_merge, state.windows, fn window ->
        # this returns a tuple with the value and new map, which then is treated as the appropriate result for map_reduce
        Map.get_and_update! state.windows, window, fn _ -> :pop end
      end

    {hold_states, trigger_states, new_elements_states, _last_pane_states, reducer_states} =
      Enum.reduce window_states, {[], [], [], []},
        fn {h, t, e, _p, r}, {hs, ts, es, _ps, rs} -> {[h | hs], [t | ts], [e | es], nil, [r | rs]} end

    new_reducer_state = merge_reducer_states reducer_states, result_window, state

    # we are guaranteed that if `result_window` already has state, then it is also part of `windows_to_merge`
    # hence it is valid to merge hold states into a brand new state, instead of having to separate out the result_window state
    # todo check if this is true.
    blank_hold_state = WatermarkHoldManager.init()
    new_hold_state = WatermarkHoldManager.merge(hold_states, blank_hold_state, result_window, state.liwm, state.lowm, state.windowing_strategy)

    new_new_elements_state = Enum.reduce new_elements_states, &||/2 # reduce with logical or

    new_last_pane_state = :none # We track fired panes only per actual window. So for a new window, we reset the count.

    old_trigger_state = nil #TODO!!!!! refactor

    {timer_cmds, new_trigger_state} = state.trigger_driver.merge(trigger_states, old_trigger_state, state.liwm) #todo: work out getting old state, check validity of liwm param
    # todo process timers

    windows = Map.put windows, result_window, {new_hold_state, new_trigger_state, new_new_elements_state, new_last_pane_state, new_reducer_state}

    %{state | windows: windows}
  end

  defp merge_reducer_states(reducer_states, window, state) do
    combined = Enum.reduce reducer_states, fn state_element, state_acc ->
      Map.merge state_acc, state_element, fn
        key, {:"$merging", states}, state2 -> {:"$merging", [state2 | states]}
        key, state1, state2 -> {:"$merging", [state2, state1]}
      end
    end

    Enum.map(combined, fn
      {key, {:"$merging", states}} -> {key, state.reducer.merge_accumulators(states, {key, window, state.windowing, []})}
      key_state -> key_state
    end)
    |> Enum.into(%{})
  end

  defp process_elements(state, elements, mapping) do
    {_, new_state} = Enum.reduce elements, {mapping, state}, &process_element/2
    new_state
  end

  defp process_element({{key, value}, timestamp, [], _}, _) do
    raise "An unwindowed element was encountered."
  end

  defp process_element({{key, value}, timestamp, [window], el_opts} = element, {mapping, state}) do
    actual_window = get_window(mapping, window)

    window_state =
      Map.get_lazy state.windows, actual_window, fn ->
        # no existing state, we need to initialise it
        new_window_state(state, actual_window)
      end

    case window_state do
      :closed ->
        Logger.debug fn -> "Dropping element <#{inspect element}> due to closed window #{inspect actual_window}" end
        {mapping, state}
      {hold_state, trigger_state, _new_el_state, pane_state, reducer_state} ->
        # process reducer
        new_reducer_state = state.reducer.process_value(value, {key, actual_window, state.windowing, [], el_opts}, reducer_state)

        # process pane tracking
        new_new_el_state = true

        new_pane_state = pane_state # pane state only changes on firing

        # process holds
        {_hold, new_hold_state} = WatermarkHoldManager.add_holds(hold_state, timestamp, actual_window, state.liwm, state.lowm, state.windowing_strategy)

        #todo assert that holds have a proximate timer

        # process trigger
        {timer_cmds, new_trigger_state} = state.trigger_driver.process_element(trigger_state, timestamp, state.liwm)
        # todo process timers


        new_window_state = {new_hold_state, new_trigger_state, new_new_el_state, new_pane_state, new_reducer_state}

        windows = Map.put state.windows, actual_window, new_window_state
        {mapping, %{state | windows: windows}}
    end


  end

  defp process_element({{key, value}, timestamp, windows, opts}, state) do
    # there is more than one window, so unpack
    Enum.reduce windows, state, fn window, st -> process_element {{key, value}, timestamp, [window], opts}, st end
  end

  defp get_window(%{}, window), do: window

  defp get_window(mapping, window) do
    Map.get mapping, window, window
  end

  defp new_window_state(state, window) do
    hold_state = WatermarkHoldManager.init()
    trigger_state = state.trigger_driver.init(state.trigger, window)
    new_elements_state = false
    pane_state = :none
    reducer_state = state.reducer.init(state.transform)

    {hold_state, trigger_state, new_elements_state, pane_state, reducer_state}
  end

end
