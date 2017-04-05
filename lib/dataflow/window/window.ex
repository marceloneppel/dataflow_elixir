defmodule Dataflow.Window do
  @moduledoc """
  Note: this module corresponds to the `BoundedWindow` classes in the Java and Python SDKs.

  A `Window` represents a finite grouping of elements, with an upper bound (since larger timestamps represent more
  recent data) on the timestamps of elements that can be placed in the window. This finiteness means that for every
  window, at some point in time, all data for that window will have arrived and can be processed together.
  """

  alias Dataflow.Utils.Time
  alias Dataflow.Window.WindowingStrategy
  require Time

  @type global :: :global
  @type interval :: Time.interval
  @type t :: global | interval

  @doc "Returns the inclusive upper bound of timestamps for values in this window."
  @spec max_timestamp(window :: t) :: Time.timestamp
  def max_timestamp(:global) do
    # Note, in the Java implementation they effectively do `max - 1 days` to get around some triggering logic.
    # Here, we need to make sure to appropriately modify that logic in the triggering code.
    Time.max_timestamp
  end

  def max_timestamp(interval) do
    Time.latest_timestamp(interval)
  end

  @doc "Returns the garbage collection time of the window, given a `WindowingStrategy`."
  @spec gc_time(window :: t, WindowingStrategy.t) :: Time.timestamp
  def gc_time(:global, _), do: Time.max_timestamp

  def gc_time(window, strategy) do
    window
    |> max_timestamp()
    |> Time.add(strategy.allowed_lateness)
  end

  @doc "Returns the global window."
  @spec global :: global
  def global, do: :global

  @doc "Checks whether the argument is the global window."
  @spec global?(any) :: boolean
  def global?(arg), do: arg == :global

  @doc """
  Returns an interval window with the given parameters. You can either pass a timestamp or a duration for the second
  argument.
  """
  @spec interval(Time.timestamp, Time.timestamp | Time.duration) :: interval
  def interval(from, to_or_for) do
    Time.interval(from, to_or_for)
  end

  @doc """
  Checks if a window is an interval window.
  """
  @spec interval?(any) :: boolean
  def interval?(term), do: Time.interval?(term)

  @doc """
  Returns the start of a window, inclusive.
  """
  @spec start_time(t) :: Time.timestamp
  def start_time({:interval, tstart, _}), do: tstart

  @doc """
  Returns the end of a window, exclusive.
  """
  @spec end_time(t) :: Time.timestamp
  def end_time({:interval, _, tend}), do: tend

  @doc """
  Checks whether the first window contains the second.
  """
  @spec contains?(t, t) :: boolean
  def contains?(:global, _), do: true
  def contains?(_, :global), do: false # if first arg is :global, previous head catches that
  def contains?(i1, i2), do: Time.contains?(i1, i2)

  @doc """
  Checks whether the two windows are disjoint.
  """
  @spec disjoint?(t, t) :: boolean
  def disjoint?(:global, _), do: false
  def disjoint?(_, :global), do: false
  def disjoint?(i1, i2), do: Time.disjoint?(i1, i2)

  @doc """
  Checks if the two windows intersect.
  """
  @spec intersect?(t, t) :: boolean
  def intersect?(w1, w2), do: not disjoint?(w1, w2)

  @doc """
  Returns the minimal window that includes both this window and the given window.
  """
  @spec span(t, t) :: t
  def span(:global, _), do: :global
  def span(_, :global), do: :global
  def span(w1, w2), do: Time.span(w1, w2)
end
