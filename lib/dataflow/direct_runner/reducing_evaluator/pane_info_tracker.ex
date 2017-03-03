defmodule Dataflow.DirectRunner.ReducingEvaluator.PaneInfoTracker do
  @moduledoc """
  Determine the timing and other properties of a new pane for a given computation and window.

  Incorporates any previous pane, whether the pane has been produced because an
  on-time `AfterWatermark` trigger firing, or the relation between the element's timestamp
  and the current output watermark.
  """

  alias Dataflow.Window
  alias Dataflow.Window.PaneInfo
  alias Dataflow.Utils.Time
  require Time
  require Logger

  @typep previous_pane :: PaneInfo.t | :none
  @typep window_max_timestamp :: Time.timestamp

  @spec get_next(window, previous_pane, output_watermark, input_watermark, last?) :: PaneInfo.t
    when window: Window.t, previous_pane: PaneInfo.t | :none,
         output_watermark: Time.timestamp | :none, input_watermark: Time.timestamp, last?: boolean
  def get_next(window, previous_pane, out_wm, in_wm, last?) do
    max_win_ts = Window.max_timestamp window
    first? = (previous_pane == :none)
    {index, non_spec_index} =
      if first? do
        {0, 0}
      else
        {previous_pane.index + 1, previous_pane.non_speculative_index + 1}
      end

    # True if it is not possible to assign the element representing this pane a timestamp
    # which will make an :on_time pane for any following computation.
    # I.e. true if the element's latest possible timestamp is before the current output watermark.
    late_for_output? = out_wm != :none && Time.before?(max_win_ts, out_wm)

    # True if all emitted panes (if any) were :early panes.
    # Once the :on_time pane has fired, all following panes must be considered :late even
    # if the output watermark is behind the end of the window.
    only_early_panes_so_far? = previous_pane == :none || previous_pane.timing == :early

    # True if the input watermark hasn't passed the window's max timestamp.
    early_for_input? = Time.before_eq?(in_wm, max_win_ts)

    timing =
      cond do
        late_for_output? || not only_early_panes_so_far? ->
          # The output watermark has already passed the end of this window, or we have already
          # emitted a non-:early pane. Irrespective of how this pane was triggered we must
          # consider this pane :late.
          :late
        early_for_input? -> :early # This is an :early firing
        true -> :on_time # This is the unique :on_time firing for the window
      end

    non_spec_index = if timing == :early do -1 else non_spec_index end # :early panes are speculative

    # todo trace and check invariants?

    pane = %PaneInfo{first?: first?, last?: last?, timing: timing, index: index, non_speculative_index: non_spec_index}

    {pane, {max_win_ts, pane}}
  end


end
