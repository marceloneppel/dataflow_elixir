defmodule Dataflow.DirectRunner.ReducingEvaluator do
  use Dataflow.DirectRunner.TransformEvaluator, type: :reducing

  alias Dataflow.{Window, Utils.Time, PValue, Window.WindowingStrategy, Trigger}
  alias Dataflow.DirectRunner.ReducingEvaluator.{PaneInfoTracker, TriggerDriver, WatermarkHoldManager}
  alias Dataflow.Window.WindowFn.Callable, as: WindowFnP
  alias Dataflow.Window.OutputTimeFn
  alias Dataflow.Window.PaneInfo
  alias Dataflow.DirectRunner.TimingManager, as: TM
  require Time
  require Logger

  defmodule Reducer do
    @type t :: module
    @type state :: any

    @type context :: {key :: any, Window.t, WindowingStrategy.t, timing_manager :: pid}

    @callback init(transform) :: state when transform: Dataflow.PTransform.t
    @callback process_value(value, context, el_attrs :: keyword, state) :: state when value: any
    @callback merge_accumulators([state], context) :: state
    @callback emit(pane_info, context, state) :: {[element], state} when element: any, pane_info: any
    @callback clear_accumulator(state) :: state

    def module_for(%Dataflow.Transforms.Core.GroupByKey{}), do: Dataflow.DirectRunner.Reducers.GBKReducer
    def module_for(%Dataflow.Transforms.Core.CombinePerKey{}), do: Dataflow.DirectRunner.Reducers.CombineReducer

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
      timing_manager: pid
    }

    defstruct \
      windows: %{},
      windowing: nil,
      reducer: nil,
      trigger_driver: nil,
      trigger: nil,
      merging?: true,
      transform: nil,
      timing_manager: nil
  end

  def init(transform, input, timing_manager) do
    {:ok,
      %State{
        windowing: input.windowing_strategy,
        reducer: Reducer.module_for(transform),
        merging?: not WindowFnP.non_merging?(input.windowing_strategy.window_fn),
        transform: transform,
        trigger_driver: TriggerDriver.module_for(input.windowing_strategy.trigger),
        trigger: input.windowing_strategy.trigger,
        timing_manager: timing_manager
      }
     }
  end

  def transform_elements(elements, state) do
    {mapping, state} = maybe_merge_windows(elements, state)

    state = state |> process_elements(elements, mapping)

    # we're done processing holds
    TM.refresh_hold(state.timing_manager)


    {[], state}
  end

  def fire_timers(timers, state) do
    Enum.reduce timers, {[], state}, &fire_timer/2
  end

  defp fire_timer({{window, trigger_id}, time, domain} = timer, {elements_acc, state}) do
    :event_time = domain

    {elements, state} =
      if timer_gc?(timer, state.windowing) do
        {elements, state} =
          if window_active_open?(window, state) do
            {new_hold,elements, state} = on_trigger(window, true, timer_eow?(timer), true, state) # doesn't matter if we discard since we turn around and clear it, but may as well
            :none = new_hold
            {elements, state}
          else
            {[], state}
          end

        # New hold should be :none since we are done with the window

        # clear all state for window since we won't see elements for it again
        windows = Map.delete state.windows, window
        {elements, %{state | windows: windows}}
      else
        {_, trigger_data, _, _, _} = state.windows[window]

        {elements, state} =
          if window_active_open?(window, state) && state.trigger_driver.should_fire?(trigger_data) do
            emit(window, state)
          else
            {[], state}
          end

        if timer_eow?(timer) do
          # If the window strategy trigger includes a watermark trigger then at this point
          # there should be no data holds, either because we'd already cleared them on an
          # earlier on_trigger, or because we just cleared them on the above emit.
          # We could assert this but it is very expensive.

          # Since we are processing an on-time firing we should schedule the garbage collection
          # timer. (If getAllowedLateness is zero then the timer event will be considered a
          # cleanup event and handled by the above).
          # Note we must do this even if the trigger is finished so that we are sure to cleanup
          # any final trigger finished bits.
          # TODO check if this applies also with this implementation

          gc_time = Window.gc_time(window, state.windowing)
          TM.set_timer(state.timing_manager, {window, :cleanup}, gc_time, :event_time)
        end

        {elements, state}
      end

      {elements ++ elements_acc, state}
  end

  defp emit(window, state) do
    # Inform the trigger of the transition to see if it is finished
    {hold_data, trigger_data, new_elements?, last_pane, element_data} = state.windows[window]
    new_trigger_state = state.trigger_driver.fired(trigger_data)
    finished? = state.trigger_driver.finished?(new_trigger_state)

    discard? = should_discard?(finished?, state.windowing)

    {_new_hold, elements, state} = on_trigger(window, finished?, false, discard?, state)

    # on trigger has cleared panes and discarded elements, if required

    window_data =
      if finished? do
        # mark window as closed but don't delete it yet
        :closed
      else
        {hold_data, new_trigger_state, new_elements?, last_pane, element_data}
      end

    state = put_in(state.windows[window], window_data)

    {elements, state}
  end

  defp should_discard?(finished?, strategy)

  defp should_discard?(true, _), do: true
  defp should_discard?(_, :discarding), do: true
  defp should_discard?(_, _), do: false

  defp on_trigger(window, finished?, eow?, discard?, state) do
    liwm = get_liwm state
    lowm = get_lowm state

    {hold_data, trigger_data, new_elements?, last_pane, element_data} = state.windows[window]

    new_pane = PaneInfoTracker.get_next(window, last_pane, liwm, lowm, finished?)

    {old_hold, new_hold, new_hold_state} =
      WatermarkHoldManager.extract_and_release(hold_data, window, liwm, lowm, state.windowing, finished?)

    TM.update_hold(state.timing_manager, window, new_hold)

    out_tm = old_hold

    empty? = not new_elements?

    # todo assertions here about the correctness of new hold etc

    # Only emit a pane if it has data or empty panes are observable
    {elements, new_element_data} =
      if should_emit?(empty?, finished?, new_pane.timing, state.windowing.closing_behavior) do
        {elements, element_data_list} =
          element_data
          |> Enum.map(&trigger_reducer(&1, state, window, out_tm, new_pane, discard?))
          |> Enum.unzip

        element_data = Map.new(element_data_list)
        elements = Enum.concat(elements)
        {elements, element_data}
      else
        {[], element_data}
      end

    state = put_in(state.windows[window], {new_hold_state, trigger_data, false, new_pane, new_element_data})

    {new_hold, elements, state}
  end

  defp trigger_reducer({key, reducer_state}, state, window, out_tm, pane, discard?) do
    {elements, new_reducer_state} =
      state.reducer.emit(pane, {key, window, state.windowing, state.timing_manager}, reducer_state)

    new_reducer_state =
      cond do
        discard? -> state.reducer.clear_accumulator(new_reducer_state)
        true -> new_reducer_state
      end

    elements =
      elements
      |> Enum.map(fn value -> {{key, value}, out_tm, [window], [pane_info: pane]} end)

    {elements, {key, new_reducer_state}}
  end

  defp should_emit?(empty?, finished?, timing, closing_behavior)

  defp should_emit?(false, _, _, _), do: true
  defp should_emit?(_, _, :on_time, _), do: true
  defp should_emit?(_, true, _, :fire_always), do: true
  defp should_emit?(_, _, _, _), do: false

  defp timer_eow?({{window, trigger_id}, time, domain}) do
    domain == :event_time && time == Window.max_timestamp window
  end

  defp timer_gc?({{window, trigger_id}, time, domain}, strategy) do
    gc_time = Window.gc_time(window, strategy)
    Time.after_eq?(time, gc_time)
  end

  defp window_active_open?(window, state) do
    not (state.windows[window] in [nil, :closed])
    &&
    (
      {_, trigger_state, _, _, _} = state.windows[window]
      not state.trigger_driver.finished?(trigger_state)
    )
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

    liwm = get_liwm state
    lowm = get_lowm state

    {new_hold, new_hold_state} = WatermarkHoldManager.merge([hold_state], hold_state, result_window, liwm, lowm, state.windowing) # possibly need to recalculate holds in new window
    new_trigger_state = state.trigger_driver.merge([trigger_state], trigger_state)

    TM.remove_hold(state.timing_manager, window_to_merge)
    TM.update_hold(state.timing_manager, result_window, new_hold)


    # todo need to recalculate trigger state maybe?

    new_window_state = {new_hold_state, new_trigger_state, new_elements_state, :none, reducer_state} # discard pane state

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

    liwm = get_liwm state
    lowm = get_lowm state

    # we are guaranteed that if `result_window` already has state, then it is also part of `windows_to_merge`
    # hence it is valid to merge hold states into a brand new state, instead of having to separate out the result_window state
    # todo check if this is true.
    blank_hold_state = WatermarkHoldManager.init()
    {new_hold, new_hold_state} = WatermarkHoldManager.merge(hold_states, blank_hold_state, result_window, liwm, lowm, state.windowing)

    TM.remove_holds(state.timing_manager, windows_to_merge)
    TM.update_hold(state.timing_manager, result_window, new_hold)

    new_new_elements_state = Enum.reduce new_elements_states, &||/2 # reduce with logical or

    new_last_pane_state = :none # We track fired panes only per actual window. So for a new window, we reset the count.

    blank_trigger_state = state.trigger_driver.init(state.trigger, result_window, state.timing_manager)

    new_trigger_state = state.trigger_driver.merge(trigger_states, blank_trigger_state, liwm)

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
      {key, {:"$merging", states}} -> {key, state.reducer.merge_accumulators(states, {key, window, state.windowing, state.timing_manager})}
      key_state -> key_state
    end)
    |> Enum.into(%{})
  end

  defp process_elements(state, elements, mapping) do
    liwm = get_liwm state
    lowm = get_lowm state
    Enum.reduce elements, state, &process_element(&1, mapping, liwm, lowm, &2)
  end

  defp process_element({{key, value}, timestamp, [], _}, _, _, _, _) do
    raise "An unwindowed element was encountered."
  end

  defp process_element({{key, value}, timestamp, [window], el_opts} = element, mapping, liwm, lowm, state) do
    actual_window = get_window(mapping, window)

    window_state =
      Map.get_lazy state.windows, actual_window, fn ->
        # no existing state, we need to initialise it
        new_window_state(state, actual_window)
      end

    case window_state do
      :closed ->
        Logger.debug fn -> "Dropping element <#{inspect element}> due to closed window #{inspect actual_window}" end
        state
      {hold_state, trigger_state, _new_el_state, pane_state, element_states} ->
        old_reducer_state = element_states[key] || new_reducer_state(state)
        # process reducer
        new_reducer_state = state.reducer.process_value(value, {key, actual_window, state.windowing, state.timing_manager}, el_opts, old_reducer_state)

        new_element_states = put_in(element_states[key], new_reducer_state)

        # process pane tracking
        new_new_el_state = true

        new_pane_state = pane_state # pane state only changes on firing

        # process holds
        {hold, new_hold_state} = WatermarkHoldManager.add_holds(hold_state, timestamp, actual_window, liwm, lowm, state.windowing)

        TM.update_hold(state.timing_manager, window, hold)

        #todo assert that holds have a proximate timer

        # process trigger
        new_trigger_state = state.trigger_driver.process_element(trigger_state, timestamp)
        # todo process timers


        new_window_state = {new_hold_state, new_trigger_state, new_new_el_state, new_pane_state, new_element_states}

        put_in(state.windows[actual_window], new_window_state)
    end


  end

  defp process_element({{key, value}, timestamp, windows, opts}, mapping, liwm, lowm, state) do
    # there is more than one window, so unpack
    Enum.reduce windows, state, fn window, st -> process_element {{key, value}, timestamp, [window], opts}, mapping, liwm, lowm, st end
  end

  defp get_window(%{}, window), do: window

  defp get_window(mapping, window) do
    Map.get mapping, window, window
  end

  defp new_window_state(state, window) do
    hold_state = WatermarkHoldManager.init()
    trigger_state = state.trigger_driver.init(state.trigger, window, state.timing_manager)
    new_elements_state = false
    pane_state = :none
    reducer_state = %{}

    {hold_state, trigger_state, new_elements_state, pane_state, reducer_state}
  end

  defp new_reducer_state(state) do
    state.reducer.init(state.transform)
  end

  defp get_liwm(state) do
    TM.get_liwm(state.timing_manager)
  end

  defp get_lowm(state) do
    TM.get_lowm(state.timing_manager)
  end

end
