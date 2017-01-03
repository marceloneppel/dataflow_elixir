defmodule Dataflow.Utils.Time do
  @moduledoc """
  In Dataflow, time is represented as an integer of UNIX microseconds, though at the user layer the code deals with a
  floating-point representation of seconds. We also distinguish between timestamps and durations. Timezone concerns
  are irrelevant. Intervals are considered to be of the form [start, end), that is they include the start but not the
  end.
  """

  @type type :: :timestamp | :duration
  @opaque time :: non_neg_integer | :max | :min
  @type t :: {type, time}
  @type timestamp :: {:timestamp, time}
  @type duration :: {:duration, time}
  @type interval :: {:interval, time, time}
  @type unit :: :seconds | :milliseconds | :microseconds

  @spec timestamp(non_neg_integer, non_neg_integer) :: timestamp
  def timestamp(seconds, microseconds \\ 0)
  def timestamp(seconds, microseconds) when is_integer(microseconds) do
    {:timestamp, seconds * 1_000_000 + microseconds}
  end

  @spec timestamp(non_neg_integer, unit) :: timestamp
  def timestamp(i, unit) when is_atom(unit) do
    {:timestamp, to_microseconds(i, unit)}
  end

  @spec duration(non_neg_integer, non_neg_integer) :: duration
  def duration(seconds, microseconds \\ 0)
  def duration(seconds, microseconds) when is_integer(microseconds) do
    {:duration, seconds * 1_000_000 + microseconds}
  end

  @spec duration(non_neg_integer, unit) :: duration
  def duration(i, unit) when is_atom(unit) do
    {:duration, to_microseconds(i, unit)}
  end

  @spec interval(timestamp, timestamp) :: interval
  def interval({:timestamp, tstart}, {:timestamp, tend}) do
    {:interval, tstart, tend}
  end

  @spec interval(timestamp, duration) :: interval
  def interval({:timestamp, tstart}, {:duration, duration}) do
    {:interval, tstart, add(tstart, duration)}
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

  @spec interval?(any) :: boolean
  def interval?({:interval, tstart, tend}) when is_integer(tstart) and is_integer(tend) do
    tstart >= 0 && tend >= 0
  end

  def interval?(_), do: false

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

  @spec less_than?(t, t) :: boolean
  def less_than?(t1, t2), do: compare(t1, t2) and t1 != t2

  @spec before?(timestamp, timestamp) :: boolean
  def before?(t1, t2), do: less_than?(t1, t2)

  @spec greater_than?(t, t) :: boolean
  def greater_than?(t1, t2), do: not compare(t1, t2)

  @spec after?(timestamp, timestamp) :: boolean
  def after?(t1, t2), do: greater_than?(t1, t2)

  @spec less_than_eq?(t, t) :: boolean
  def less_than_eq?(t1, t2), do: compare(t1, t2)

  @spec before_eq?(timestamp, timestamp) :: boolean
  def before_eq?(t1, t2), do: less_than_eq?(t1, t2)

  @spec greater_than_eq?(t, t) :: boolean
  def greater_than_eq?(t1, t2), do: not less_than?(t1, t2)

  @spec after_eq?(timestamp, timestamp) :: boolean
  def after_eq?(t1, t2), do: greater_than_eq?(t1, t2)



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

  @spec add(timestamp, duration) :: timestamp
  def add({:timestamp, t}, {:duration, d}) do
    {:timestamp, t + d}
  end

  @spec add(duration, duration) :: duration
  def add({:duration, d1}, {:duration, d2}) do
    {:duration, d1 + d2}
  end

  def add(_, {:timestamp, _}) do
    raise ArgumentError, message: "Cannot add a timestamp to something."
  end

  @spec subtract(timestamp, duration) :: timestamp
  @spec subtract(duration, duration) :: duration
  def subtract(t1, {:duration, d}) do
    add(t1, {:duration, -d})
    #todo add underflow check???
  end

  @spec multiply(duration, integer) :: duration
  def multiply({:duration, d}, i) when is_integer(i) do
    {:duration, d * i}
  end


  @doc """
  Returns the raw representation of the given time object, for non-orthodox operations. Use the second parameter
  to select the unit returned. Note that an integer will always be returned, and the result rounded down if using a
  coarser unit.
  """
  @spec raw(t) :: integer
  @spec raw(t, unit) :: integer

  def raw(t, units \\ :microseconds)

  def raw({:timestamp, t}, :microseconds), do: t
  def raw({:duration, t}, :microseconds), do: t

  def raw(t, :milliseconds) do
    raw(t, :microseconds)
    |> div(1_000)
  end

  def raw(t, :seconds) do
    raw(t, :microseconds)
    |> div(1_000_000)
  end

  @spec to_microseconds(integer, unit) :: integer
  defp to_microseconds(i, :microseconds), do: i
  defp to_microseconds(i, :milliseconds), do: i * 1_000
  defp to_microseconds(i, :seconds), do: i * 1_000_000

  @doc """
  Returns the latest timestamp which can be included in an interval, i.e. the _inclusive_ end of the interval.
  """
  @spec latest_timestamp(interval) :: timestamp
  def latest_timestamp({:interval, _tstart, tend}) do
    tend - 1
  end

  @doc """
  Returns whether the first interval contains the second interval.
  """
  @spec contains?(interval, interval) :: boolean
  def contains?({:interval, tstart1, tend1}, {:interval, tstart2, tend2}) do
    before_eq?(tstart1, tstart2) && after_eq?(tend1, tend2)
  end

  @doc """
  Returns whether the two intervals are disjoint.
  """
  @spec disjoint?(interval, interval) :: boolean
  def disjoint?({:interval, tstart1, tend1}, {:interval, tstart2, tend2}) do
    before_eq?(tend1, tstart2) || before_eq?(tend2, tstart1)
  end

  @doc """
  Returns whether the two intervals intersect.
  """
  @spec intersect?(interval, interval) :: boolean
  def intersect?(i1, i2), do: not disjoint?(i1, i2)

  @doc """
  Returns the minimal window that includes both windows.
  """
  @spec span(interval, interval) :: interval
  def span({:interval, tstart1, tend1}, {:interval, tstart2, tend2}) do
    {:interval, Kernel.min(tstart1, tstart2), Kernel.max(tend1, tend2)}
  end
end
