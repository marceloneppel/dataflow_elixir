defmodule Dataflow.Transforms.Core.CombineGlobally do
  use Dataflow.PTransform, make_fun: [combine_globally: 1]

  defstruct combine_fn: nil #todo tags? enforce keys

  def combine_globally(combine_fn) do
    %__MODULE__{combine_fn: combine_fn}
  end

  defimpl PTransform.Callable do
    alias Dataflow.Transforms.Core.CombineGlobally

    def apply(%CombineGlobally{combine_fn: fun}, input) do
        #todo defaults, as_view
        use Dataflow.Transforms.Core.CombinePerKey
        import Dataflow.Transforms.Core, only: [map: 1]

        combined =
        input
        #~> "KeyWithVoid" -- add_input_types(...)
        ~> "CombinePerKey" -- combine_per_key(fun)
        ~> "UnKey" -- map(fn {_k, v} -> v end)
        combined
      end
  end
end
