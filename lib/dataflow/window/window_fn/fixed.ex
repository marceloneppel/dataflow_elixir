defmodule Dataflow.Window.WindowFn.Fixed do
  @moduledoc """
  A windowing function that assigns each element to one time interval.

  The fields size and offset determine in what time interval a timestamp
  will be slotted. The time intervals have the following formula:
  [N * size + offset, (N + 1) * size + offset)

  Attributes:
    size: Size of the window as seconds.
    offset: Offset of this window as seconds since Unix epoch. Windows start at
      t=N * size + offset where t=0 is the epoch. The offset must be a value
      in range [0, size).
  """

  alias Dataflow.Window.WindowFn

  defstruct size: nil, offset: nil

  alias Dataflow.Utils.Time
  import Dataflow.Utils, only: [mod: 2]

  @type t :: %__MODULE__{
    size: Time.duration,
    offset: Time.duration
  }

  def from_opts(opts) do
    {size_amt, size_unit} = Keyword.fetch!(opts, :size)
    size = Time.duration(size_amt, size_unit)
    offset =
      case opts[:offset] do
        nil -> Time.duration(0)
        {amt, unit} -> Time.duration(amt, unit)
      end

    new(size, offset)
  end

  def new(size, offset \\ Time.duration(0)) do
    unless Time.greater_than_eq?(offset, Time.duration(0)) && Time.less_than?(offset, size) do
      raise ArgumentError, "Fixed windows must have 0 <= offset < size"
    end

    %__MODULE__{size: size, offset: offset}
  end

  defimpl WindowFn.Callable do
    use WindowFn
    alias Dataflow.Window.WindowFn.Fixed

    def non_merging?(_), do: true

    def assign(%Fixed{size: size, offset: offset}, timestamp, _element, _windows) do
      # calculate how far into some window the timestamp is
      overlap =
        timestamp
        |> Time.add(size)
        |> Time.subtract(offset)
        |> Time.raw
        |> mod(Time.raw(size))
        |> Time.duration(:microseconds)

      start = Time.subtract(timestamp, overlap)

      [Dataflow.Window.interval(start, size)]
    end

    def side_input_window(fun, window) do
      if Dataflow.Window.global? window do
        raise ArgumentError, message: "Attempted to get side input window for GlobalWindow from non-global WindowFn"
      end

      assign(fun, Dataflow.Window.max_timestamp(window), nil, nil) |> List.first()
    end
  end
end
