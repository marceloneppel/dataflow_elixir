defmodule Dataflow.Transforms.Core do
  alias Dataflow.PTransform

  def flat_map(fun) when is_function(fun) do
   #todo labels?
   fun
   |> Dataflow.Transforms.Util.DoFn.from_function
   |> Dataflow.Transforms.Core.ParDo.par_do
  end

  def map(fun) when is_function(fun) do
    #todo side inputs
    wrapper = fn x -> [fun.(x)] end
    #todo labelling
    #todo type hints

    flat_map(wrapper)
  end

  def filter(fun) when is_function(fun) do
    #todo typings, labelling

    wrapper = fn x -> if fun.(x), do: [x], else: [] end

    flat_map(wrapper)
  end
end
