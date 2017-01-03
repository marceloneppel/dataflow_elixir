defmodule Dataflow.Transforms.Fns.WindowFn do
  @moduledoc """
  The argument to the `Dataflow.Transforms.???.Window` transform used to assign elements into windows and to determine
  how windows are merged. See that module for more information on how these are used.

  Users will generally want to use the predefined `WindowFn`s, but it is also possible to create new ones.

  To create a custom `WindowFn`, implement `Dataflow.Transforms.Fns.WindowFn.Callable`, or use this module for default
  implementations.

  NonMerging and Partitioning tags to be added later.
  """

  alias Dataflow.{Utils, Window}

  @doc """
  Associates a set of windows to an element.

  `timestamp` is the timestamp currently assigned to the element.

  `element` is the element itself.

  `existing_windows` is a list of windows currently assigned to the element prior to this `WindowFn` being called.
  """
  @callback assign(window_fn :: struct, timestamp :: Utils.Time.timestamp, element :: any, existing_windows :: [Window.t])
              :: [Window.t]

  @doc """
  Allows the merging of some windows. To indicate that several windows should be merged into a new one, include a
  `{to_be_merged, merge_result}` tuple in the output list. To indicate no merging, just return the empty list. Any lists
  of windows returned must be mutually disjoint.

  The default implementation returns the empty list.
  """
  @callback merge(window_fn :: struct, windows :: [Window.t]) :: [{[Window.t], Window.t}]

  @doc """
  Returns the window of the side input corresponding to the given window of the main input.
  """
  @callback side_input_window(window_fn :: struct, window :: Window.t) :: Window.t

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
  @callback transformed_output_time(window_fn :: struct, window :: Window.t, input_timestamp :: Utils.Time.t)
              :: Utils.Time.t

  @doc """
  Is true if this `WindowFn` never needs to merge any windows.

  Default implementation returns false.
  """
  @callback non_merging?(window_fn :: struct) :: boolean

  defmacro __using__(opts) do
    this_module = __MODULE__
    caller_module = Macro.expand(__MODULE__, __CALLER__)
    fields = Keyword.get opts, :fields, []
    this_protocol = __MODULE__.Callable

    quote bind_quoted: [this_module: this_module, caller_module: caller_module, fields: fields, this_protocol: this_protocol] do
      @behaviour this_module

      defstruct fields

      def transformed_output_time(_, _window, timestamp), do: timestamp

      def non_merging?(_), do: false

      def merge(_, _windows), do: []

      defoverridable [transformed_output_time: 3, non_merging?: 1, merge: 2]

      defimpl this_protocol do
        def assign(fun, timestamp, element, existing_windows), do: caller_module.assign(fun, timestamp, element, existing_windows)
        def merge(fun, windows), do: caller_module.merge(fun, windows)
        def side_input_window(fun, window), do: caller_module.side_input_window(fun, window)
        def transformed_output_time(fun, window, input_timestamp), do: caller_module.get_transformed_output_time(fun, window, input_timestamp)
        def non_merging?(fun), do: caller_module.non_merging?(fun)
      end
    end
  end
end
