defmodule Dataflow.DirectRunner.ReducingEvaluator do
  use Dataflow.DirectRunner.TransformEvaluator, type: :reducing

  alias Dataflow.{Window, Utils.Time, PValue, Window.WindowingStrategy}
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
      trigger: TriggerDriver.instance,
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
        transform: transform
      }
     }
  end

  def transform_elements(elements, state) do
    {mapping, state} = maybe_merge_windows(elements, state)

    state = state |> process_elements(elements, mapping)

    # now check for trigger firings

    {[], :no_update, state}
  end

  def update_input_watermark(watermark, state) do
    # TODO
    {[], :no_update, state}
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

  defp merge_windows({[window_to_merge], result}, state) do
    # only one window to merge, so just do a replacement

    # get and remove state of window_to_merge
    {window_state, windows} = Map.get_and_update! state.windows, window_to_merge, fn _ -> :pop end

    {hold_state, trigger_state, new_elements_state, _last_pane_state, reducer_state} = window_state

    new_window_state = {hold_state, trigger_state, new_elements_state, :none, reducer_state} # discard pane state

    windows = Map.put windows, result, new_window_state

    %{state | windows: windows}
  end

  defp merge_windows({to_merge, result}, state) do
    # todo check invariants

    # get window states, and remove them from the map at the same time
    {window_states, windows} =
      Enum.map_reduce to_merge, state.windows, fn window ->
        # this returns a tuple with the value and new map, which then is treated as the appropriate result for map_reduce
        Map.get_and_update! state.windows, window, fn _ -> :pop end
      end

    {hold_states, trigger_states, new_elements_states, _last_pane_states, reducer_states} =
      Enum.reduce window_states, {[], [], [], []},
        fn {h, t, e, _p, r}, {hs, ts, es, _ps, rs} -> {[h | hs], [t | ts], [e | es], nil, [r | rs]} end

    new_reducer_state = merge_reducer_states reducer_states, result, state

    new_hold_state = nil #TODO

    new_new_elements_state = Enum.reduce new_elements_states, &||/2 # reduce with logical or

    new_last_pane_state = :none # We track fired panes only per actual window. So for a new, window, we reset the count.

    new_trigger_state = nil #TODO

    windows = Map.put windows, result, {new_hold_state, new_trigger_state, new_new_elements_state, new_last_pane_state, new_reducer_state}

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

  defp process_element({{key, value}, timestamp, []}, _) do
    raise "An unwindowed element was encountered."
  end

  defp process_element({{key, value}, timestamp, [window]}, {mapping, state}) do
    actual_window = get_window(mapping, window)

    window_state =
      Map.get_lazy state.windows, actual_window, fn ->
        # no existing state, we need to initialise it
        new_window_state(state)
      end

    case window_state do
      :closed ->
        Logger.debug "Dropping element due to closed window"
        {mapping, state}
      {hold_state, trigger_state, _el_state, pane_state, reducer_state} ->
        # process reducer
        new_reducer_state = state.reducer.process_value(value, {key, actual_window, state.windowing, []}, reducer_state)

        # process pane tracking
        new_el_state = true

        new_pane_state = pane_state

        # process holds
        new_hold_state = hold_state

        # process timers?

        # process trigger
        new_trigger_state = trigger_state

        new_window_state = {new_hold_state, new_trigger_state, new_el_state, new_pane_state, new_reducer_state}

        windows = Map.put state.windows, actual_window, new_window_state
        {mapping, %{state | windows: windows}}
    end


  end

  defp process_element({{key, value}, timestamp, windows}, state) do
    # there is more than one window, so unpack
    Enum.reduce windows, state, fn window, st -> process_element {{key, value}, timestamp, [window]}, st end
  end

  defp get_window(%{}, window), do: window

  defp get_window(mapping, window) do
    Map.get mapping, window, window
  end

  defp new_window_state(state) do
    reducer_state = state.reducer.init(state.transform)
    {nil, nil, false, :none, reducer_state} #TODO!!!!!
  end

end
