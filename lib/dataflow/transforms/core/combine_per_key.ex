defmodule Dataflow.Transforms.Core.CombinePerKey do
  use Dataflow.PTransform

  defstruct combine_fn: nil #todo tags? enforce keys

  def new(cfun), do: %__MODULE__{combine_fn: cfun}

  defimpl PTransform.Callable do
    alias Dataflow.Transforms.Core.CombinePerKey

    def expand(%CombinePerKey{}, input) do
      #todo labels
      fresh_pvalue input, from: input
    end
  end



end
