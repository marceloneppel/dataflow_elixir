defmodule Dataflow.Transforms.Aggregation.TopPerKey do
  use Dataflow.PTransform
  defstruct [:comparator, :size]

  # TODO reconsider whether to allow extracting the comparison key and values separately

  def new(size, opts \\ []) do
    comparator = opts[:compare] || &<=/2
    %__MODULE__{comparator: comparator, size: size}
  end

  defimpl PTransform.Callable do
    alias Dataflow.Transforms.Fns.CombineFn
    alias Dataflow.Transforms.Core
    alias Dataflow.Transforms.Aggregation.TopPerKey

    def expand(%TopPerKey{comparator: comparator, size: size}, input) do
      input
      ~> "TopPerKey" -- Core.combine_per_key(top_combine_fn(comparator, size))
    end

    defp top_combine_fn(comparator, size) do
      # TODO: implement this with a bounded heap for better efficiency
      alias Dataflow.Utils.PriorityQueue, as: PQ
      inv_cmp = invert_comparator(comparator)
      %CombineFn{
        create_accumulator: fn -> PQ.new(inv_cmp) end,
        add_input: fn q, el ->
          q
          |> PQ.put(el, nil)
          |> PQ.trim(size)
        end,
        merge_accumulators: fn q1, q2 ->
          # todo optimise
          # todo ensure that the comparator is the same

          {els, _} = PQ.take_all(q2)
          els
          |> Enum.map(fn {k, _} -> k end)
          |> Enum.reduce(q1, fn el, q -> PQ.put(q, el, nil) end)
          |> PQ.trim(size)
        end,
        extract_output: fn q ->
          {els, _} = PQ.take_all(q)
          els
          |> Enum.map(fn {k, _} -> k end)
        end
      }
    end

    defp invert_comparator(cmp) do
      if cmp == &<=/2 do
        &>=/2
      else
        inverted_comparator(cmp)
      end
    end

    defp inverted_comparator(comparator) do
      fn el1, el2 ->
        cond do
          el1 == el2 -> true
          comparator.(el1, el2) -> false # el1 <= el2, so el1 >= el2 is false
          true -> true # el1 <= el2 is false, so is el1 == el2, therefore el1 >= el2 is true
        end
      end
    end
  end
end
