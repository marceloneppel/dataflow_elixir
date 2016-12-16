defmodule Dataflow.Pipeline.AppliedTransform do

  defstruct [:pipeline, :id, :label, :input, :output, :parent, :parts, :transform]


end
