defmodule Dataflow.Transforms.Windowing do
  use Dataflow.Utils.TransformUtils

  export_transforms [
    Window: [0, 1],
    WithTimestamps: [1, 2],
    AssignTimestamps: 1,
    Watermark: [0, 1]
  ]
end
