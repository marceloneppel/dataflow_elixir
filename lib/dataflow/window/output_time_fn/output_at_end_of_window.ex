defmodule Dataflow.Window.OutputTimeFn.OutputAtEndOfWindow do
  @moduledoc """
  OutputTimeFn outputting at latest input timestamp.

  The policy of outputting with timestamps at the end of the window.

  Note that this output timestamp depends only on the window. See
  `c:Dataflow.Window.OutputTimeFn.depends_only_on_window?/0`.

  When windows merge, insead of using `c:Dataflow.Window.OutputTimeFn.combine/2` to obtain an output timestamp
  for the results in the new window, it is mandatory to obtain a new output timestamp from
  `c:Dataflow.Window.OutputTimeFn.assign_output_time/2` with the new window and an arbitrary timestamp (since it
  is guaranteed that the timestamp is irrelevant).

  For non-merging window functions, this `OutputTimeFn` works transparently.
  """

  use Dataflow.Window.OutputTimeFn

  alias Dataflow.Window

  def assign_output_time(window, _input_timestamp) do
    Window.max_timestamp window
  end

  def combine(output_timestamp, _other_output_timestamp) do
    output_timestamp
  end

  def merge(result_window, _merging_timestamps) do
    # Since we know that the result only depends on the window, we can ignore
    # the given timestamps.
    assign_output_time(result_window, :timestamp_ignored)
  end

  def depends_only_on_window?, do: true
end
