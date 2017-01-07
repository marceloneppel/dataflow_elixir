defmodule Dataflow.Transforms.Core.GroupByKeyOnly do
  use Dataflow.PTransform, make_fun: [group_by_key_only: 0]

  defstruct []

  def group_by_key_only, do: %__MODULE__{}

  defimpl PTransform.Callable do
    def expand(_, input) do
      fresh_pvalue input
    end
  end
end
