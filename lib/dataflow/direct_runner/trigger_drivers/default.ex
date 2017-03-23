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

  def process_element(%__MODULE__{window: window, timing_manager: tm} = state, _timestamp) do
    # If the end of the window has already been reached, then we are already ready to fire
    # and do not need to set a wake-up timer.
    event_time = TM.get_liwm tm
    if eow_reached?(window, event_time) do
      state
    else
      TM.set_timer(tm, {window, :default_trigger}, Window.max_timestamp(window), :event_time)
      state
    end
  end

  def merge(states, %__MODULE__{window: window, timing_manager: tm} = state) do
    # If the end of the window has already been reached, then we are already ready to fire
    # and do not need to set a wake-up timer.

    # clear any existing timers
    Enum.each states, fn %{window: window, timing_manager: tm} -> TM.clear_timers(tm, {window, :default_trigger}, :event_time) end

    event_time = TM.get_liwm tm

    if eow_reached?(window, event_time) do
      state
    else
      TM.set_timer(state.tm, {window, :default_trigger}, Window.max_timestamp(window), :event_time)
      state
    end
  end

  def should_fire?(state) do
    event_time = TM.get_liwm state.tm
    eow_reached?(state.window, event_time)
  end

  def fired(state) do
    state
  end

  def finished?(state) do
    false # todo check this
  end

  defp eow_reached?(window, time) do
    time != :none && Time.after?(time, Window.max_timestamp(window))
  end
end
