defmodule Dataflow.Transforms.Core.Create do
  @moduledoc """
  A transform that creates a PCollection from an enumerable.
  """

  use Dataflow.PTransform, make_fun: [create: 1]

  defstruct value: []

  def create(value), do: %__MODULE__{value: value}

  defimpl PTransform.Callable do
    def apply(_, input) do
      #TODO: assert that input is PBegin?
      fresh_pvalue input
    end
  end
end
