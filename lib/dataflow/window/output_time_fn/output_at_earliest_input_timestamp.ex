defmodule Dataflow.Window.OutputTimeFn.OutputAtEarliestInputTimestamp do
  @moduledoc """
  OutputTimeFn outputting at earliest input timestamp.

  The policy of outputting at the earliest of the input timestamps for non-late input data that led to a computed value.

  For example, suppose `v_i` through `v_n` are all on-time elements being aggregated via some function `f` into
  `f([v_i, ... , v_n])`. When emitted, the output timestamp of the result will be the earliest one of the event
  timestamps.

  If the data arrives late, it has no effect on the output timestamps.
  """

  use Dataflow.Window.OutputTimeFn

  def assign_output_time(_window, input_timestamp) do
    input_timestamp
  end

  def depends_only_on_earlier_input_timestamp?, do: true

end
