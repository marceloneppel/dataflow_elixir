defmodule Dataflow.PValue do
  @moduledoc """
  A PValue is a thing which can be input to and output from `PTransform`s.

  Dataflow users should not use PValue structs directly in their
  pipelines.

  A PValue has the following main characteristics:
    (1) Belongs to a pipeline. Added during object initialization.
    (2) Has a transform that can compute the value if executed.
    (3) Has a value which is meaningful if the transform was executed.
  """

  defstruct :pipeline, :id, :label, :producer, type: :normal
  #TODO enforce keys

end
