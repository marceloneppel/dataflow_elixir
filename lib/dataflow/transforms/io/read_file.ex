defmodule Dataflow.Transforms.IO.ReadFile do
  @moduledoc """
  Reads a file as a collection of lines.

  *Testing only* pending the implementation of the source/sink API.
  """

  use Dataflow.PTransform

  defstruct filename: nil

  def new(filename), do: %__MODULE__{filename: filename}

  defimpl PTransform.Callable do
    alias Dataflow.Transforms.IO.ReadFile

    def expand(%ReadFile{}, input) do
      #TODO: assert input is a dummy?
      fresh_pvalue input
    end
  end
end
