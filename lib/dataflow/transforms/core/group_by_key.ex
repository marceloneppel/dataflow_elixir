defmodule Dataflow.Transforms.Core.GroupByKey do
  use Dataflow.PTransform, make_fun: {:group_by_key, 0}

  def group_by_key, do: :ok
end
