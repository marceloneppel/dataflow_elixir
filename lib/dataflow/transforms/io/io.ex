defmodule Dataflow.Transforms.IO do
  use Dataflow.Utils.TransformUtils

  export_transforms [
    Create: 1,
    ReadFile: 1,
    WriteFile: 1,
    ReadStream: [1, 2]
  ]
end
