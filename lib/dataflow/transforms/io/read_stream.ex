defmodule Dataflow.Transforms.IO.ReadStream do
  use Dataflow.PTransform
  defstruct [:make_stream, :chunk_size]

  def new(make_stream, chunk_size \\ 100) do
    %__MODULE__{make_stream: make_stream, chunk_size: chunk_size}
  end

  defimpl PTransform.Callable do

    def expand(_, input) do
      fresh_pvalue(input)
    end
  end
end
