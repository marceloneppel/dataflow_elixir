defmodule Dataflow.Transforms.Windowing.Watermark do
  use Dataflow.PTransform
  use Dataflow.Utils.Time

  defstruct [:delay]

  def new(opts \\ []) do
    delay =
      case opts[:delay] do
        nil -> Time.duration(0)
        {amt, unit, :event_time} -> Time.duration(amt, unit) # For now, only implement event time delay
      end

    %__MODULE__{delay: delay}
  end

  defimpl PTransform.Callable do
    def expand(_, input) do
      fresh_pvalue input, from: input # manage the execution in the evaluator
    end
  end
end
