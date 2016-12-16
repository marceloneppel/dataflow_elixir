defmodule Dataflow.Transforms.Core.CombineValues do
  use Dataflow.PTransform, make_fun: [combine_values: 1]

  defstruct combine_fn: nil #todo tags? enforce keys
  alias Dataflow.Transforms.Util.CombineFn

  def apply(%__MODULE__{combine_fn: %CombineFn{} = fun}, input) do
    use Dataflow.Transforms.Core.ParDo

    #todo typings
    input
    ~> par_do(combine_do_fn(fun))
  end

  def combine_values(fun), do: %__MODULE__{combine_fn: fun}

  defp combine_do_fn(%CombineFn{} = fun) when is_function(fun) do
    alias Dataflow.Transforms.Util.DoFn

    %DoFn{process:
      fn {key, enum}, timestamp, windows, label, state ->
        # Expected elements input to this DoFn are 2-tuples of the form
        # `{key, enum}`, with `iter` an enumerable of all the values associated with `key`
        # in the input PCollection.

        combined = CombineFn.create_accumulator(fun)
        |> CombineFn.add_inputs(fun, enum)
        |> CombineFn.extract_output(fun)

        {key, combined}
      end
    }
  end
end
