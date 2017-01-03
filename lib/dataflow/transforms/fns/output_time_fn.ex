defmodule Dataflow.Transforms.Fns.OutputTimeFn do
  @moduledoc """
  A function from timestamps of input values to the timestamp for a computed value.

  The function is represented via three components:

    * `assign_output_time/2` calculates an output timestamp for any input value in a particular window.

    * The output timestamps for all non-late input values within a window are combined
      according to `combine/2`, a commutative and associative operation on
      the output timestamps.

    * The output timestamp when windows merge is provided by `merge/2`

  Defaults are provided for all callbacks apart from `c:assign_output_time/2`. To use normal defaults, simply use this
  module:

  ```
  use Dataflow.Transforms.Fns.OutputTimeFn
  ```
  """

  alias Dataflow.{Window, Utils}

  @doc """
  Returns the output timestamp to use for data depending on the given `input_timestamp` in the specified `window`.

  The result of this method must be between `input_timestamp` and the maximum timestamp of the `window`
  (inclusive on both sides).

  This function must be monotonic across input timestamps, that is, if `t1` is before `t2`, then
  `assign_output_time(t1, window)` is before `assign_output_time(t2, window)`.

  For a `WindowFn` that doesn't produce overlapping windows, this can (and typically should) just return
  `input_timestamp`. In the presence of overlapping windows, it is suggested that the later windows don't prevent the
  watermark from progressing past the end of the earlier window.

  Consistency properties required of `assign_output_time`, `c:combine/2` and `c:merge/2` are documented in
  `OutputTimeFn`.
  """
  @callback assign_output_time(window :: Window.t, input_timestamp :: Utils.Time.timestamp)
              ::  Utils.Time.timestamp

  @doc """
  Combines the given output times, which must be from the same window, into an output time for a computed value.

  `combine` must be commutative: `combine(a, b) == combine(b, a)`.

  `combine` must be associative: `combine(a, combine(b, c)) == combine(combine(a, b), c)`.

  The default implementation returns the earliest timestamp.
  """
  @callback combine(output_timestamp :: Utils.Time.timestamp, other_output_timestamp :: Utils.Time.timestamp)
              :: Utils.Time.timestamp

  @doc """
  Combines all of the given output times at once. The default implementation is a reduction using `c:combine/2`, but
  this can be used for certain optimisations.
  """
  @callback combine_all(merging_timestamps :: [Utils.Time.timestamp]) :: Utils.Time.timestamp

  @doc """
  Merges the given output times, presumed to be combined output times for windows that are merging, into an output time
  for the `result_window`.

  When windows `w1` and `w2` merge to become a new window `w1plus2` then `merge/2` must be implemented such that the
  output time is the same as if all the timestamps were assigned in `w1plus2`. Formally,

  ```
  Fn.merge(w1plus2, [Fn.assign_output_time(t1, w1), Fn.assign_output_time(t2, w2)])
  ```

  must be equal to

  ```
  Fn.combine(Fn.assign_output_time(t1, w1plus2), Fn.assign_output_time(t2, w1plus2))
  ```

  If the assigned time depends only on the window, the correct implementation of `merge` necessarily returns the result
  of `assign_output_time(t1, w1plus2)`, which must equal `assign_output_time(t2, w1plus2)`.

  For many other `OutputTimeFn` implementations, such as taking the earliest or latest timestamp, this will be the same
  as `c:combine_all/1`, and indeed this is the default implementation.
  """
  @callback merge(result_window :: Window.t, merging_timestamps :: [Utils.Time.timestamp]) :: Utils.Time.timestamp

  @doc """
  Returns `true` if the result of combination of many output timestamps actually depends only on the earliest.

  This may allow optimisations when it is very efficient to retrieve the earliest timestamp to be combined.

  The default implementation returns `false`.
  """
  @callback depends_only_on_earlier_input_timestamp? :: boolean

  @doc """
  Returns `true` if the result does not depend on what outputs were combined but only the window they are in. The
  canonical example is if all the timestamps are sure to be the end of the window.

  This may allow optimisations, since it is typically very efficient to retrieve the window and combining output
  timestamps is not necessary.

  If the assigned output time for an implementation depends only on the window, consult the code for
  `OutputTimeFn.DependsOnlyOnWindow` and possibly reuse some of its functions.

  The default implementation returns `false`.
  """
  @callback depends_only_on_window? :: boolean

  defmacro __using__(_opts) do
    quote do
      @behaviour unquote(__MODULE__)

      def combine(output_timestamp, other_output_timestamp) do
        Utils.Time.min(output_timestamp, other_output_timestamp)
      end

      def combine_all(merging_timestamps) do
        Enum.reduce merging_timestamps, &combine/2
      end

      def merge(_result_window, merging_timestamps) do
        combine_all(merging_timestamps)
      end

      def depends_only_on_earlier_input_timestamp?, do: false

      def depends_only_on_window?, do: false

      defoverridable [combine: 2, combine_all: 1, merge: 2, depends_only_on_earlier_input_timestamp?: 0, depends_only_on_window?: 0]
    end
  end
end
