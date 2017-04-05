defmodule Dataflow.Window.WindowFn.Sessions do
  @moduledoc """
  A windowing function that groups elements into sessions.

  A session is defined as a series of consecutive events
  separated by a specified gap size.

  Fields:
    `gap_size`: Size of the gap between windows.
  """

  alias Dataflow.Window.WindowFn

  defstruct gap_size: nil

  alias Dataflow.Utils.Time
  alias Dataflow.Window

  @type t :: %__MODULE__{
    gap_size: Time.duration
  }

  def from_opts(opts) do
    {size_amt, size_unit} = Keyword.fetch!(opts, :gap_size)
    size = Time.duration(size_amt, size_unit)

    new(size)
  end

  @spec new(gap_size :: Time.duration) :: t
  def new(gap_size) do
    unless Time.greater_than?(gap_size, Time.duration(0)) do
      raise ArgumentError, "The gap size must be strictly positive."
    end

    %__MODULE__{gap_size: gap_size}
  end

  defimpl WindowFn.Callable do
    use WindowFn
    alias Dataflow.Window.WindowFn.Sessions

    def assign(%Sessions{gap_size: gap_size}, timestamp, _element, _windows) do
      [Dataflow.Window.interval(timestamp, gap_size)]
    end

    def non_merging?(_), do: false

    def merge(_, windows) do
      # Sort the windows by start time
      # For performance assume that we have interval windows.
      # TODO verify the unwrapping functions aren't too slow

      # First we sort the windows by start time
      windows
      |> Enum.sort_by(&Window.start_time/1, &Time.compare/2)
      # Then we chunk overlapping ones together
      |> chunk_overlapping_windows
    end

    defp chunk_overlapping_windows(windows) do
      windows
      |> Enum.reduce([], &chunk_reducer/2)
      |> Enum.map(&finish_chunking/1)
    end

    # A reducer to chunk overlapping windows together
    # Accumulator has shape {current_span, current_to_merge, result}
    defp chunk_reducer(window, []) do
      # Initial accumulator
      {window, [window], []}
    end

    defp chunk_reducer(window, {current_span, current_to_merge, result}) do
      if Window.intersect?(window, current_span) do
        # The current window overlaps with the span of the ones so far, so add it to the current acc
        {Window.span(current_span, window), [window | current_to_merge], result}
      else
        # The current window is disjoint from the span of the preceding ones
        # Start a new chunk
        {window, [window], [{current_to_merge, current_span} | result]}
      end
    end

    defp finish_chunking({current_span, current_to_merge, result}) do
      [{current_to_merge, current_span} | result]
    end

    def side_input_window(_, _) do
      raise "Sessions is not allowed in side inputs"
    end
  end


end
