defmodule Dataflow do
  defmacro __using__(_opts) do
    quote do
      alias Dataflow.Pipeline

      import Dataflow, only: [~>: 2]
      import Dataflow.Pipeline, only: [pvalue?: 1, valid_pvalue?: 1]

      import Dataflow.Transforms.Core, only: [dummy_root_xform: 0, dummy_xform: 0] # TODO make this conditional on opts
    end
  end

  def (%Dataflow.PValue{pipeline: p} = value) ~> transform do
    Dataflow.Pipeline.apply_transform p, value, transform
  end

  def (%Dataflow.Pipeline{} = p) ~> transform do
    Dataflow.Pipeline.apply_root_transform(p, transform)
  end

  def (%Dataflow.NestedInput{} = value) ~> transform do
    Dataflow.Pipeline.apply_nested_transform value, transform
  end
end
