defmodule Dataflow.Transforms.Fns.WindowFn do

  @doc """
  Associates a timestamp and set of windows to an element.
  """
  @callback assign(timestamp, element, existing_windows)

  @doc """
  Returns a window that is the result of merging a set of windows.
  """
  @callback merge(windows)

  @doc """
  Given input time and output window, returns output time for window.

  If OutputTimeFn.OUTPUT_AT_EARLIEST_TRANSFORMED is used in the Windowing,
  the output timestamp for the given window will be the earliest of the
  timestamps returned by get_transformed_output_time() for elements of the
  window.
  """
  @callback get_transformed_output_time(window, input_timestamp)

  def default_get_transformed_output_time(_window, input_timestamp) do
    # By default, just return the input timestamp.
    input_timestamp
  end
end
