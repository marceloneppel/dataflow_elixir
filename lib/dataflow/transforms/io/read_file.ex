defmodule Dataflow.Transforms.IO.ReadFile do
  @moduledoc """
  Reads a file as a collection of lines.

  *Testing only* pending the implementation of the source/sink API.
  """

  use Dataflow.PTransform, make_fun: [read_file: 1]

  defstruct filename: nil

  def read_file(filename), do: %__MODULE__{filename: filename}

  defimpl PTransform.Callable do
    alias Dataflow.Transforms.IO.ReadFile

    def expand(%ReadFile{filename: fname}, input) do
      #TODO: assert input is a dummy?
      fresh_pvalue input
    end
  end
end
