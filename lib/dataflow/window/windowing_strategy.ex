defmodule Dataflow.Window.WindowingStrategy do
  require Dataflow.Trigger
  require Dataflow.Utils.Time

  defstruct \
    window_fn: %Dataflow.Window.WindowFn.Global{},
    output_time_fn: Dataflow.Window.OutputTimeFn.OutputAtEndOfWindow,
    trigger: Dataflow.Trigger.default,
    accumulation_mode: :discarding,
    allowed_lateness: Dataflow.Utils.Time.zero_duration,
    closing_behavior: :fire_if_non_empty

  defmacro default_strategy do
    quote do
      %Dataflow.Window.WindowingStrategy{

      }
    end
  end
end
