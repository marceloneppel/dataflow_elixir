defmodule Dataflow.Transforms.Aggregation.CountElements do
  use Dataflow.PTransform

  defstruct []

  def new, do: %__MODULE__{}

  defimpl PTransform.Callable do
    alias Dataflow.Transforms.{Aggregation, Core}

    def expand(_, input) do
      input
      ~> "ElementsToKeys" -- Core.map(fn x -> {x, nil} end)
      ~> Aggregation.count_per_key()
    end
  end
end
