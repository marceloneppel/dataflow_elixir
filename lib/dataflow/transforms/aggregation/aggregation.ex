defmodule Dataflow.Transforms.Aggregation do
  use Dataflow.Utils.TransformUtils

  export_transforms [
    CountPerKey: 0,
    TopPerKey: [1, 2],
    CountElements: 0
  ]

end
