defmodule Dataflow.Utils.Time do
  @moduledoc """
  In Dataflow, time is represented as an integer of UNIX microseconds, though at the user layer the code deals with a
  floating-point representation of seconds. We also distinguish between timestamps and durations.
  """

  @type type :: :timestamp | :duration
  @opaque time :: non_neg_integer | :max | :min
  @type t :: {type, time}
  @type timestamp :: {:timestamp, time}
  @type duration :: {:duration, time}

  @spec timestamp(non_neg_integer, non_neg_integer) :: timestamp
  def timestamp(seconds, microseconds \\ 0) do
    {:timestamp, seconds * 1_000_000 + microseconds}
  end

  @spec duration(non_neg_integer, non_neg_integer) :: duration
  def duration(seconds, microseconds \\ 0) do
    {:duration, seconds * 1_000_000 + microseconds}
  end

  @spec timestamp?(any) :: boolean
  def timestamp?({:timestamp, t}) when is_integer(t) do
    t >= 0
  end

  def timestamp?(_), do: false

  @spec duration?(any) :: boolean
  def duration?({:duration, t}) when is_integer(t) do
    t >= 0
  end

  def duration?(_), do: false

  @spec max_timestamp :: timestamp
  def max_timestamp, do: {:timestamp, :max}

  @spec min_timestamp :: timestamp
  def min_timestamp, do: {:timestamp, :min}

  @spec compare(t, t) :: boolean
  def compare({:timestamp, t1}, {:timestamp, t2}) do
    t1 <= t2
  end

  def compare({:duration, t1}, {:duration, t2}) do
    t1 <= t2
  end

  def compare({:timestamp, _}, {:duration, _}) do
    raise ArgumentError, message: "Cannot compare timestamps with durations."
  end

  def compare({:duration, _}, {:timestamp, _}) do
    raise ArgumentError, message: "Cannot compare timestamps with durations."
  end

  @spec min(t, t) :: t
  def min(a, b) do
    if compare(a, b) do
      a
    else
      b
    end
  end

  @spec max(t, t) :: t
  def max(a, b) do
    if compare(a, b) do
      b
    else
      a
    end
  end

end
