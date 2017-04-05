defmodule Dataflow.Window.WindowFn do
  @moduledoc """
  The argument to the `Dataflow.Transforms.Windowing.Window` transform used to assign elements into windows and to determine how windows are merged.

  See that module for more information on how these are used.

  Users will generally want to use the predefined `WindowFn`s, but it is also possible to create new ones.

  To create a custom `WindowFn`, implement `Dataflow.Transforms.Fns.WindowFn.Callable`, optionally using this module for
  default implementations.

  NonMerging and Partitioning tags to be added later.
  """


  defmacro __using__(_opts) do
    quote do

      def transformed_output_time(_, _window, timestamp), do: timestamp
      def non_merging?(_), do: false
      def merge(_, _windows), do: []

      defoverridable [transformed_output_time: 3, non_merging?: 1, merge: 2]

    end
  end

  alias Dataflow.Window.WindowFn.{Fixed, Global, Sessions, Sliding}

  def parse(window) when is_atom(window), do: parse({window, []})
  def parse({:sliding, opts}) do
    Sliding.from_opts(opts)
  end

  def parse({:global, _opts}) do
    Global.new()
  end

  def parse({:sessions, opts}) do
    Sessions.from_opts(opts)
  end

  def parse({:fixed, opts}) do
    Fixed.from_opts(opts)
  end

  def parse(%{__struct__: mod} = window) do
    Protocol.assert_impl!(mod, Callable)
    window
  end

  defprotocol Callable do
    alias Dataflow.{Utils, Window}

    @doc """
    Associates a set of windows to an element.

    `timestamp` is the timestamp currently assigned to the element.

    `element` is the element itself.

    `existing_windows` is a list of windows currently assigned to the element prior to this `WindowFn` being called.
    """
    @spec assign(
            window_fn :: t,
            timestamp :: Utils.Time.timestamp,
            element :: any,
            existing_windows :: [Window.t]
          ) :: [Window.t]
    def assign(window_fn, timestamp, element, existing_windows)

    @doc """
    Allows the merging of some windows.

    To indicate that several windows should be merged into a new one, include a
    `{to_be_merged, merge_result}` tuple in the output list. To indicate no merging, just return the empty list. Any lists
    of windows returned must be mutually disjoint.

    For performance reasons it is safe to return no-op merges such as `{[w], w}`, these will be ignored.

    The default implementation returns the empty list.
    """
    @spec merge(window_fn :: t, windows :: [Window.t]) :: [{[Window.t], Window.t}]
    def merge(window_fn, windows)

    @doc """
    Returns the window of the side input corresponding to the given window of the main input.
    """
    @spec side_input_window(window_fn :: t, window :: Window.t) :: Window.t
    def side_input_window(window_fn, window)

    @doc """
    Returns the output timestamp to use for data depending on the given `input_timestamp` in the specified `window`.

    The result of this method must be between `input_timestamp` and the maximal timestamp of the `window` (both sides
    inclusive).

    This function must be monotonic across input timestamps.

    For a `WindowFn` that does not produce overlapping windows, this can (and typically should) just return
    `input_timestamp`. In the presence of overlapping windows, it is suggested that the result in later overlapping
    windows is past the end of earlier windows so that the later windows don't prevent the watermark from progressing
    past the end of the earlier window.
    """
    # TODO: is transformed_output_time deprecated? It's only found in the Python SDK
    @spec transformed_output_time(window_fn :: t, window :: Window.t, input_timestamp :: Utils.Time.t)
                :: Utils.Time.t
    def transformed_output_time(window_fn, window, input_timestamp)

    @doc """
    Is true if this `WindowFn` never needs to merge any windows.

    Default implementation returns false.
    """
    @spec non_merging?(window_fn :: t) :: boolean
    def non_merging?(window_fn)
  end
end
