defmodule Dataflow.Transforms.Core.ParDo do
  use Dataflow.PTransform

  defstruct do_fn: nil #todo side output tags? enforce keys?

  def new(fun), do: %__MODULE__{do_fn: fun}

  defimpl PTransform.Callable do
    #TODO with_outputs
    def expand(_, input) do
      #TODO init side output tags
      fresh_pvalue input, from: input
    end
  end

end
