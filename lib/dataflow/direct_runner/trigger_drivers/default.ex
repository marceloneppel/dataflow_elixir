defmodule Dataflow.DirectRunner.TriggerDrivers.Default do
  @behaviour Dataflow.DirectRunner.ReducingEvaluator.TriggerDriver

  alias Dataflow.{Trigger, Window, Utils.Time}
  require Time

  alias Dataflow.DirectRunner.TimingManager, as: TM

  defstruct [:window, :timing_manager]

  def init(%Trigger.Default{}, window, tm) do
    %__MODULE__{
      window: window,
      timing_manager: tm
    }
  end

  def process_element(%__MODULE__{window: window, timing_manager: tm} = state, _timestamp, event_time) do
    # If the end of the window has already been reached, then we are already ready to fire
    # and do not need to set a wake-up timer.
    if eow_reached?(window, event_time) do
      state
    else
      TM.set_timer(tm, :system_trigger, window, Window.max_timestamp(window), :event_time)
      state
    end
  end

  def merge(windows, window, event_time) do
    # If the end of the window has already been reached, then we are already ready to fire
    # and do not need to set a wake-up timer.
    if eow_reached?(window, event_time) do
      {[], window}
    else
      {[{:set, Window.max_timestamp(window), :event_time}], window} # todo save in state whether we've already done this?
    end
  end

  def should_fire?(window, event_time) do
    eow_reached?(window, event_time)
  end

  def fired(window, _time) do
    window
  end

  def finished?(window) do
    false # todo check this
  end

  defp eow_reached?(window, time) do
    time != :none && Time.after?(time, Window.max_timestamp(window))
  end
end
