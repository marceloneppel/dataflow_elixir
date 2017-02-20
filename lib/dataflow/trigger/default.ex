defmodule Dataflow.Trigger.Default do
  defstruct []

  defimpl Dataflow.Trigger.Triggerable do
    alias Dataflow.Utils.Time
    require Time

    def watermark_that_guarantees_firing(_, _window), do: Time.max_timestamp()
    def continuation_trigger(_), do: %Dataflow.Trigger.Default{}
  end
end
