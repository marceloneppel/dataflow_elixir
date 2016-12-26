defmodule Dataflow.Transforms.Fns.OutputTimeFn.OutputAtEarliestTransformedInputTimestamp do
  @moduledoc "OutputTimeFn outputting at earliest transformed input timestamp."

  #TODO FINISH

  use Dataflow.Transforms.Fns.OutputTimeFn

  def assign_output_time(_window, input_timestamp) do
    raise "module not implemented correctly yet."
    input_timestamp
  end

  def combine(output_timestamp, other_output_timestamp) do
    output_timestamp
  end

end
