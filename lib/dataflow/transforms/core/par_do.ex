defmodule Dataflow.Transforms.Core.ParDo do
  use Dataflow.PTransform, make_fun: {:par_do, 1}

  defstruct do_fn: nil #todo side output tags? enforce keys?
  #TODO with_outputs
  def apply(%__MODULE__{}, input) do
    #TODO init side output tags
    fresh_pvalue input
  end

  def par_do(fun), do: %__MODULE__{do_fn: fun}
end
