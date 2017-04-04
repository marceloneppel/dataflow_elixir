defmodule Dataflow.Transforms.Core.GroupByKey do
  use Dataflow.PTransform

  defstruct []

  def new, do: %__MODULE__{}

  defimpl PTransform.Callable do

    def expand(_, input) do
      fresh_pvalue input, from: input
    end

  end

end
