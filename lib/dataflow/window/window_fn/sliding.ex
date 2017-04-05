defmodule Dataflow.Window.WindowFn.Sliding do
  @moduledoc """
  A windowing function that windows values into possibly overlapping fixed-size timestamp-based windows.

  The attributes size and offset determine in what time interval a timestamp
  will be slotted. The time intervals have the following formula:
  [N * period + offset, N * period + offset + size)

  Fields:
    `size`: Size of the window as seconds.
    `period`: Period of the windows as seconds.
    `offset`: Offset of this window as seconds since Unix epoch. Windows start at
      t=N * period + offset where t=0 is the epoch. The offset must be a value
      in range [0, period).
  """

  alias Dataflow.Window.WindowFn
  alias Dataflow.Utils.Time

  defstruct size: nil, offset: nil, period: nil

  @type t :: %__MODULE__{
    size: Time.duration,
    offset: Time.duration,
    period: Time.duration
  }

  def from_opts(opts) do
    {size_amt, size_unit} = Keyword.fetch!(opts, :size)
    {period_amt, period_unit} = Keyword.fetch!(opts, :period)
    size = Time.duration(size_amt, size_unit)
    period = Time.duration(period_amt, period_unit)
    offset =
      case opts[:offset] do
        nil -> Time.duration(0)
        {amt, unit} -> Time.duration(amt, unit)
      end

    new(size, period, offset)
  end

  @spec new(size :: Time.duration, period :: Time.duration, offset :: Time.duration) :: t
  def new(size, period, offset \\ Time.duration(0)) do
    unless Time.greater_than_eq?(offset, Time.duration(0)) && Time.less_than?(offset, size) do
      raise ArgumentError, "Sliding windows must have 0 <= offset < size"
    end

    %__MODULE__{size: size, period: period, offset: offset}
  end

  defimpl WindowFn.Callable do
    use WindowFn
    alias Dataflow.Window.WindowFn.Sliding

    def non_merging?(_), do: true

    def assign(%Sliding{size: size, offset: offset, period: period}, timestamp, _element, _windows) do
      import Dataflow.Utils, only: [mod: 2]

      # Get the last starting time of a window that contains this timestamp.
      overlap =
        timestamp
        |> Time.add(period)
        |> Time.subtract(offset)
        |> Time.raw
        |> mod(Time.raw(period))
        |> Time.duration(:microseconds)

      last_starting_time = Time.subtract(timestamp, overlap)

      min_first_starting_time = Time.subtract(last_starting_time, size)

      last_starting_time
      |> Stream.iterate(&Time.subtract(&1, period))
      |> Stream.take_while(&Time.after_eq?(&1, min_first_starting_time))
      |> Enum.map(fn start -> Dataflow.Window.interval(start, size) end)
    end

    def side_input_window(fun, window) do
      if Dataflow.Window.global? window do
        raise ArgumentError, message: "Attempted to get side input window for GlobalWindow from non-global WindowFn"
      end

      #assign(fun, Dataflow.Window.max_timestamp(window), nil, nil) |> List.first()
      # todo check this
      raise "Not implemented yet."
    end
  end


end
