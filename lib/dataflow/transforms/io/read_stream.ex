defmodule Dataflow.Transforms.IO.ReadStream do
  use Dataflow.PTransform
  defstruct [:make_stream, :chunk_size, :full_elements?]

  def new(make_stream, opts \\ []) do
    chunk_size = opts[:chunk_size] || 100
    full_elements? = opts[:full_elements?] || false
    %__MODULE__{make_stream: make_stream, chunk_size: chunk_size, full_elements?: full_elements?}
  end

  defimpl PTransform.Callable do

    def expand(_, input) do
      fresh_pvalue(input)
    end
  end
end
