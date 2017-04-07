defmodule Dataflow.Transforms.Core.CombineValues do
  use Dataflow.PTransform

  defstruct combine_fn: nil #todo tags? enforce keys
  alias Dataflow.Transforms.Fns.CombineFn
  alias Dataflow.Transforms.Core

  def new(fun), do: %__MODULE__{combine_fn: fun}

  defimpl PTransform.Callable do
    alias Dataflow.Transforms.Core.CombineValues

    def expand(%CombineValues{combine_fn: %CombineFn{} = fun}, input) do

      #todo typings
      input
      ~> Core.map(combine(fun))
    end


    defp combine(%CombineFn{} = fun) do
      alias Dataflow.Transforms.Fns.DoFn


      fn {key, enum} ->
        # Expected elements input to this DoFn are 2-tuples of the form
        # `{key, enum}`, with `iter` an enumerable of all the values associated with `key`
        # in the input PCollection.

        combined =
          fun
          |> CombineFn.p_create_accumulator
          |> CombineFn.p_add_inputs(enum)
          |> CombineFn.p_extract_output

        {key, combined}
      end

    end
  end

end
