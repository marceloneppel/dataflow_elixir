defmodule Dataflow.Transforms.Core.GroupByKey do
  use Dataflow.PTransform, make_fun: [group_by_key: 0]

  defstruct []

  alias Dataflow.Utils.WindowedValue
  alias Dataflow.Transforms.Fns.DoFn

  def group_by_key, do: %__MODULE__{}

  defimpl PTransform.Callable do

    def expand(_, input) do
      fresh_pvalue input
    end

  end

end
