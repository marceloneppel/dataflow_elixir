defmodule Dataflow.Transforms.IO.WriteFile do
  @moduledoc """
  Writes a colletion of lines to a file.

  *Testing only* pending the implementation of the source/sink API.
  """

  use Dataflow.PTransform

  defstruct filename: nil

  def new(filename), do: %__MODULE__{filename: filename}

  defimpl PTransform.Callable do
    alias Dataflow.Transforms.IO.WriteFile

    def expand(%WriteFile{}, input) do
      fresh_pvalue input, type: :dummy
    end
  end
end
