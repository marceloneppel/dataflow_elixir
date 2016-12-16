defmodule Dataflow do
  defmacro __using__(_opts) do
    quote do
      alias Dataflow.Pipeline

      import Dataflow, only: [~>: 2]
      import Dataflow.Pipeline, only: [pvalue?: 1, valid_pvalue?: 1]

    end
  end

  def (%Dataflow.PValue{pipeline: p} = value) ~> transform do
    Dataflow.Pipeline.apply_transform p, value, transform
  end

  def (%Dataflow.Pipeline{} = p) ~> transform do
    Dataflow.Pipeline.apply_root_transform(p, transform)
  end

  def (%Dataflow.Pipeline.NestedInput{} = value) ~> transform do
    Dataflow.Pipeline.apply_nested_transform value, transform
  end
end
