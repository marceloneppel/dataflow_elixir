defmodule Dataflow do
  defmacro __using__(_opts) do
    quote do
      alias Dataflow.Pipeline

      require Dataflow
      import Dataflow, only: [~>: 2, --: 2]
      import Kernel, except: [--: 2]
    end
  end

  def (%Dataflow.PValue{pipeline: p} = value) ~> {label, transform} when is_binary(label) do
    Dataflow.Pipeline.apply_transform p, value, transform, label: label
  end

  def (%Dataflow.PValue{pipeline: p} = value) ~> transform do
    Dataflow.Pipeline.apply_transform p, value, transform
  end


  def (%Dataflow.Pipeline{} = p) ~> {label, transform} when is_binary(label) do
    Dataflow.Pipeline.apply_root_transform p, transform, label: label
  end

  def (%Dataflow.Pipeline{} = p) ~> transform do
    Dataflow.Pipeline.apply_root_transform p, transform
  end


  def (%Dataflow.Pipeline.NestedInput{} = value) ~> {label, transform} when is_binary(label) do
    Dataflow.Pipeline.apply_nested_transform value, transform, label: label
  end

  def (%Dataflow.Pipeline.NestedInput{} = value) ~> transform do
    Dataflow.Pipeline.apply_nested_transform value, transform
  end



  defmacro label -- transform, do: {label, transform}
end
