defmodule Dataflow.IO.BoundedSource do
  @moduledoc """
  A `Source` that reads a finite amount of input.

  *Note: This implementation currently supports only bounded sources.*

  *TODO: splitting, parallelism, etc.*
  """

  defprotocol Readable do
    @spec start_reader(t) :: any
    def start_reader(source)
  end
end
