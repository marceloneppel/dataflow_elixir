defmodule Dataflow.Transforms.Core.CombineGlobally do
  use Dataflow.PTransform

  defstruct combine_fn: nil #todo tags? enforce keys

  alias Dataflow.Transforms.Core

  def new(combine_fn) do
    %__MODULE__{combine_fn: combine_fn}
  end

  defimpl PTransform.Callable do
    alias Dataflow.Transforms.Core.CombineGlobally

    def expand(%CombineGlobally{combine_fn: fun}, input) do
        #todo defaults, as_view

        combined =
        input
        #~> "KeyWithVoid" -- add_input_types(...)
        ~> "CombinePerKey" -- Core.combine_per_key(fun)
        ~> "UnKey" -- Core.map(fn {_k, v} -> v end)
        combined
      end
  end
end
