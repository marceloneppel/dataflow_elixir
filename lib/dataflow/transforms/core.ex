defmodule Dataflow.Transforms.Core do
  alias Dataflow.PTransform

  defmodule ParDo do
    defstruct :fun #todo side output tags? enforce keys?
    #TODO with_outputs

    def apply(%__MODULE__{}, input) do
      #TODO init side output tags

      PTransform.new_pvalue input
    end
  end

  def flat_map(fun) when is_function(fun) do
   #todo labels?
   %ParDo{fun: fun}
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

  defmodule CombineGlobally do
    defstruct :fun #todo tags? enforce keys

    def apply(%__MODULE__{fun: fun}, input) do
      #todo defaults, as_view

      combined =
      input
      #~> "KeyWithVoid" -- add_input_types(...)
      ~> "CombinePerKey" -- combine_per_key(fun)
      ~> "UnKey" -- map(fn {k, v} -> v end)

      combined
    end
  end

  defmodule CombinePerKey do
    defstruct :fun #todo tags? enforce keys

    def apply(%__MODULE__{fun: fun}, input) do
      #todo labels

      input
      ~> group_by_key
      ~> "Combine" -- combine_values(fun)
    end
  end

  defmodule CombineValues do
    defstruct :fun #todo tags? enforce keys

    def apply(%__MODULE__{fun: fun}, input) do

      #todo typings

      input
      ~> %ParDo{fun: combine_fn(fun)}
    end

    defp combine_fn(fun) when is_function(fun) do
      #runtime behaviour different... need access to context and accumulators.
    end
  end
end
