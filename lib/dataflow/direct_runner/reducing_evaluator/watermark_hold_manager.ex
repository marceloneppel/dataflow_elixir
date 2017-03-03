defmodule Dataflow.DirectRunner.ReducingEvaluator.WatermarkHoldManager do
  alias Dataflow.Utils.Time
  alias Dataflow.Window

  require Time
  require Logger

  defstruct data_hold: :none, extra_hold: :none

  @extra_otfn Dataflow.Window.OutputTimeFn.OutputAtEarliestInputTimestamp

  def init() do
    %__MODULE__{}
  end

  @doc """
  Add a hold to prevent the output watermark progressing.

  The output watermark should not progress beyond the (possibly adjusted) timestamp of the `element`. We allow the
  actual hold time to be shited later by `OutputTimeFn.assign_output_time`, but no further than the end of the window.
  The hold will remain until cleared by `extract_and_release`. Returns the timestamp at which
  the hold was placed, or `:none` if no hold was placed (along with the new state).
  """
  def add_holds(state, timestamp, window, liwm, lowm, windowing_strategy) do
    # this is a non-conventional use of `with`. Typically we have a sequence of matches, where we want to abort on
    # failure. Here, we want to abort on success, and fall through to a fallback hold on failure. Therefore we
    # try to match on failure. If the return value is not `:none`, the `with` block will return the (successful) result.

    with {:none, state} <- add_element_hold(state, timestamp, window, liwm, lowm, windowing_strategy),
      {:none, state} <- add_eow_hold(state, window, liwm, lowm, windowing_strategy, false),
      do: add_gc_hold(state, window, liwm, lowm, windowing_strategy, false)
  end

  @doc """
  Update the watermark hold when windows merge.

  Used when it is possible the merged value does not equal all of the existing holds. For example, if the new window
  implies a later watermark hold, then earlier holds may be released.

  Parameters are the hold states of all the windows to be merged, the state of the result, the new/resultant window of
  the merge, and the windowing strategy to be used.
  """
  def merge(states, state, window, liwm, lowm, windowing_strategy) do
    state = do_merge(states, state, window, windowing_strategy)

    # we have a new data hold. Now clear the extra hold and regenerate it.
    # again the need for this is asserted in the Java implementation, and it should be verified at some point
    state = %{state | extra_hold: :none}

    {_, state} =
      with {:none, state} <- add_eow_hold(state, window, liwm, lowm, windowing_strategy, false),
        do: add_gc_hold(state, window, liwm, lowm, windowing_strategy, false)

    state
  end

  defp do_merge([], state, _window, _windowing_strategy) do
    # nothing to merge in
    state
  end

  defp do_merge(states, state, window, windowing_strategy) do
    dooeit? = windowing_strategy.output_time_fn.depends_only_on_earliest_input_timestamp?()
    doow? = windowing_strategy.output_time_fn.depends_only_on_window?()
    do_merge(states, state, window, windowing_strategy, dooeit?, doow?)
  end

  defp do_merge([state], state, _window, _ws, true, _doow?) do
    # output depends only on earliest timestamp, and there is no change in state, so do nothing.
    state
  end

  defp do_merge(_states, state, window, windowing_strategy, _dooeit?, true) do
    # output depends only on window, so just directly recalculate it
    hold = windowing_strategy.output_time_fn.assign_output_time(window, :timestamp_ignored)
    add_hold(state, :data, hold, windowing_strategy)
  end

  defp do_merge(states, state, window, windowing_strategy, _dooeit?, _doow?) do
    data_holds = for %__MODULE__{data_hold: dh} <- states, dh != :none, do: dh
    if Enum.empty? data_holds do
      # no data holds were actually set
      state
    else
      merged_hold = windowing_strategy.output_time_fn.merge(window, data_holds)
      add_hold(state, :data, merged_hold, windowing_strategy)
    end
  end

  # Attempt to add an "element hold".
  #
  # Returns the timestamp at which the hold was added, or :none if no hold was added (all along with the modified state).
  # The hold is only added if both:
  # * The backend will be able to respect it, i.e. if the output watermark is not ahead of the proposed hold time.
  # * A timer will be set to clear the hold by the end of the window, i.e. the input watermark is not ahead of the eow.
  #
  # The hold ensures the pane which incorporates the element will not be considered late by any downstream computation
  # when it is eventually emitted.
  defp add_element_hold(state, timestamp, window, liwm, lowm, windowing_strategy) do
    # Shift the hold according to the output function
    # This may only shift the hold forward, giving earlier windows a chance to close quicker.
    element_hold = windowing_strategy.output_time_fn.assign_output_time(window, timestamp)

    cond do
      lowm != :none && Time.before?(element_hold, lowm) ->
        Logger.debug fn -> "Element hold #{inspect element_hold} is too late to effect output watermark." end
        {:none, state}
      Time.before?(Window.max_timestamp(window), liwm) ->
        Logger.debug fn -> "Element hold #{inspect element_hold} is too late for end-of-window timer." end
        {:none, state}
      true ->
        {element_hold, add_hold(state, :data, element_hold, windowing_strategy)}
    end
  end

  # Attempt to add an 'end-of-window hold'.
  #
  # Returns the timestamp at which the hold was added (eow time) or :none if no eow hold is possible (as well as new state).
  #
  # We only add the hold if we can be sure a timer will be set to clear it, i.e. the input watermark is not ahead of the
  # end of window time.
  #
  # And end-of-window hold is added in two situations:
  # 1. An incoming element came in behind the output watermark (so we are too late for placing the usual element hold),
  #    but it may still be possible to include the element in an :on_time pane. We place the end of the window hold to
  #    ensure that pane will not be considered late by any downstream computation.
  # 2. We guarantee an :on_time pane will be emitted for all windows which saw at least one element, even if that :on_time
  #    pane is empty. Thus when elements in a pane are processed due to a fired trigger, we must set both an end of window
  #    timer and an end of window hold. Again, the hold ensures the :on_time pane will not be considered late by any
  #    downstream computation.
  defp add_eow_hold(state, window, liwm, lowm, windowing_strategy, _pane_empty?) do
    eow_hold = Window.max_timestamp(window)

    if Time.before?(eow_hold, liwm) do
      # too late for eow timer
      {:none, state}
    else
      # There is a good reason in the Java implementation to not use pane_empty? here to select whether we need to use
      # the extra hold, or whether we can fall back to the data hold. I'm not sure if it applies here, but I will retain
      # it for now.
      {eow_hold, add_hold(state, :extra, eow_hold, windowing_strategy)}
    end
  end

  # Attempt to add a "garbage collection hold" if it is required.
  #
  # Returns the timestamp at which the hold was added (eow time + allowed lateness) or :none if no eow hold is possible
  # (as well as new state).
  #
  # We only add the hold if it is distinct from what would be added by `add_eow_hold`, i.e. if allowed lateness is non-zero.
  #
  # A GC hold is added in two situations:
  # 1. An incoming element came in behind the output watermark, and was too late for placing the usual element hold or
  #    an eow hold. We place the GC hold so that we can guarantee that when the pane is finally triggered, its output
  #    will not be dropped due to excessive lateness by any downstream computation.
  # 2. The windowing strategy's closing behaviour is :fire_always, and thus we guarantee a final pane will be emitted
  #    for all windows which saw at least one element. Again, the GC hold guarantees that any empty final pane can be
  #    given a timestamp which will not be considered beyond allowed lateness by any downstream computation.
  #
  # `pane_empty?` is used to distinguish cases 1 and 2.
  defp add_gc_hold(state, window, liwm, lowm, windowing_strategy, pane_empty?) do
    eow = Window.max_timestamp(window)
    gc_hold = Time.add(eow, windowing_strategy.allowed_lateness)

    cond do
      gc_hold == eow ->
        # unnecessary since same as eow hold
        {:none, state}
      pane_empty? && windowing_strategy.closing_behavior == :fire_if_non_empty ->
        # unnecessary since we won't fire this pane
        {:none, state}
      true ->
        {gc_hold, add_hold(state, :extra, gc_hold, windowing_strategy)}
    end
  end

  defp add_hold(state, :data, hold, windowing_strategy) do
    new_data_hold =
      state.data_hold
      |> update_hold(hold, windowing_strategy.output_time_fn)

    %{state | data_hold: new_data_hold}
  end

  defp add_hold(state, :extra, hold, _windowing_strategy) do
    new_extra_hold =
      state.extra_hold
      |> update_hold(hold, @extra_otfn)

    %{state | extra_hold: new_extra_hold}
  end

  defp update_hold(:none, hold, _otfn) do
    hold
  end

  defp update_hold(old, new, otfn) do
    otfn.combine(old, new)
  end

end
