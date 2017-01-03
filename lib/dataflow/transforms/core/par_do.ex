defmodule Dataflow.Transforms.Core.ParDo do
  use Dataflow.PTransform, make_fun: [par_do: 1]

  defstruct do_fn: nil #todo side output tags? enforce keys?

  def par_do(fun), do: %__MODULE__{do_fn: fun}

  defimpl PTransform.Callable do
    #TODO with_outputs
    def apply(_, input) do
      #TODO init side output tags
      fresh_pvalue input
    end
  end

end