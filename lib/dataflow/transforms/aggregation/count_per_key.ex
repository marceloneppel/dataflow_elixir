defmodule Dataflow.Transforms.Aggregation.CountPerKey do
  use Dataflow.PTransform
  defstruct []

  def new, do: %__MODULE__{}

  defimpl PTransform.Callable do
    alias Dataflow.Transforms.Fns.CombineFn
    alias Dataflow.Transforms.Core

    def expand(_, input) do
      input
      ~> "CountPerKey" -- Core.combine_per_key(count_combine_fn())
    end

    defp count_combine_fn do
      %CombineFn{
        create_accumulator: fn -> 0 end,
        add_input: fn acc, _el -> acc + 1 end,
        merge_accumulators: fn acc1, acc2 -> acc1 + acc2 end,
        extract_output: fn acc -> acc end
      }
    end
  end
end
