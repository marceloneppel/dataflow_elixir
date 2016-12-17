defmodule Dataflow.Transforms.Core.CombineGlobally do
  use Dataflow.PTransform

  defstruct combine_fn: nil #todo tags? enforce keys

  def apply(%__MODULE__{combine_fn: fun}, input) do
    #todo defaults, as_view
    use Dataflow.Transforms.Core.CombinePerKey
    import Dataflow.Transforms.Core

    combined =
    input
    #~> "KeyWithVoid" -- add_input_types(...)
    ~> "CombinePerKey" -- combine_per_key(fun)
    ~> "UnKey" -- map(fn {_k, v} -> v end)
    combined
  end
end
