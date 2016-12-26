defmodule Dataflow.Transforms.Fns.OutputTimeFn.DependsOnlyOnWindow do
  @moduledoc "OutputTimeFn that only depends on the window."

  #TODO this should actually be an extensible thing in itself

  use Dataflow.Transforms.Fns.OutputTimeFn

  def combine(output_timestamp, other_output_timestamp) do
    raise "module not implemented correctly yet."
    output_timestamp
  end

  def merge(result_window, _merging_timestamps) do
    # Since we know that the result only depends on the window, we can ignore
    # the given timestamps.
    assign_output_time(result_window, nil)
  end
end
