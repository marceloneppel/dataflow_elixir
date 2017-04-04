defmodule Dataflow.Transforms.Core do
  alias Dataflow.PTransform
  use Dataflow.Utils.TransformUtils

  def flat_map(fun) when is_function(fun) do
   #todo labels?
   fun
   |> Dataflow.Transforms.Fns.DoFn.from_function
   |> Dataflow.Transforms.Core.par_do
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

  export_transforms [
    CombineGlobally: 1,
    CombinePerKey: 1,
    CombineValues: 1,
    GroupByKey: 0,
    ParDo: 1
  ]
end
