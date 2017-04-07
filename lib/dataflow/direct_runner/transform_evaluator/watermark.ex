defmodule Dataflow.DirectRunner.TransformEvaluator.Watermark do
  use Dataflow.DirectRunner.TransformEvaluator
  use Dataflow.Utils.Time

  alias Dataflow.Transforms.Windowing.Watermark, as: WatermarkXform
  alias Dataflow.DirectRunner.TimingManager, as: TM

  defstruct [:timing_manager, :delay, :high_wm]

  def timing_manager_options do
    [manage_own_lowm: true]
  end

  def init(%WatermarkXform{delay: delay}, input, timing_manager) do
    # delay is in :event_time. For now only operate on :event_time.
    {:ok, %__MODULE__{
      timing_manager: timing_manager,
      delay: delay,
      high_wm: Time.min_timestamp
    }}
  end

  def transform_elements(elements, state) do
    latest_wm = Enum.reduce elements, state.high_wm, fn {_, timestamp, _, _}, max_timestamp -> Time.later(timestamp, max_timestamp) end
    state =
      if latest_wm == state.high_wm do
        state
      else
        state = %{state | high_wm: latest_wm}
        TM.advance_desired_lowm(state.timing_manager, desired_lowm(state))
        %{state | high_wm: latest_wm}
      end

    {elements, state}
  end

  def finish(state) do
    TM.advance_desired_lowm(state.timing_manager, Time.max_timestamp) # possibly redundant?
    :ok
  end

  defp desired_lowm(%{high_wm: Time.min_timestamp}), do: Time.min_timestamp # can't delay a minimum timestamp
  defp desired_lowm(%{high_wm: high_wm, delay: delay}), do: Time.subtract(high_wm, delay)

end
