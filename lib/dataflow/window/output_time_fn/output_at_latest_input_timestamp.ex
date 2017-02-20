defmodule Dataflow.Window.OutputTimeFn.OutputAtLatestInputTimestamp do
  @moduledoc """
  OutputTimeFn outputting at latest input timestamp.

  The policy of holding the watermark to the latest of the input timestamps for non-late input data that led to a
  computed value.

  or example, suppose `v_i` through `v_n` are all on-time elements being aggregated via some function `f` into
  `f([v_i, ... , v_n])`. When emitted, the output timestamp of the result will be the latest of the event time
  timestamps.

  If data arrives late, it has no effect on the output timestamp.
  """

  use Dataflow.Window.OutputTimeFn

  alias Dataflow.Utils.Time

  def assign_output_time(_window, input_timestamp) do
    input_timestamp
  end

  def combine(output_timestamp, other_output_timestamp) do
    Time.max(output_timestamp, other_output_timestamp)
  end
end
