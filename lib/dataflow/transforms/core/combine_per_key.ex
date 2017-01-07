defmodule Dataflow.Transforms.Core.CombinePerKey do
  use Dataflow.PTransform, make_fun: [combine_per_key: 1]

  defstruct combine_fn: nil #todo tags? enforce keys

  def combine_per_key(cfun), do: %__MODULE__{combine_fn: cfun}

  defimpl PTransform.Callable do
    alias Dataflow.Transforms.Core.CombinePerKey

    def expand(%CombinePerKey{combine_fn: fun}, input) do
      use Dataflow.Transforms.Core.{GroupByKey, CombineValues}
      #todo labels
      input
      ~> group_by_key()
      ~> "Combine" -- combine_values(fun)
    end
  end



end
