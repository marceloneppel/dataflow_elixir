defmodule Dataflow.Transforms.IO.Create do
  @moduledoc """
  A transform that creates a PCollection from an enumerable.
  """

  use Dataflow.PTransform

  defstruct value: []

  def new(value), do: %__MODULE__{value: value}

  defimpl PTransform.Callable do
    def expand(_, input) do
      #TODO: assert that input is PBegin?
      fresh_pvalue input
    end
  end
end
