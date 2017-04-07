defmodule Dataflow.Transforms.Windowing.Window do
  @moduledoc """
  A `Window` logically divides up or groups the elements of a `PCollection` into finite windows according to a
  `WindowFn`. The output of the `Window` transform contains the same elements as its input, but they have been logically
  assigned to windows. The next `GroupByKey` (whether explicit or within a composite transform) will group by the
  combination of keys and windows.

  ## Windowing

  Windowing a `PCollection` divides the elements into windows based on the associated event time for each element.
  This is especially useful for `PCollections` with unbounded size, since it allows operating on a sub-group of the
  elements placed into a related window. For `PCollection`s with a bounded size (a.k.a. conventional batch mode), by
  default, all data is implicitly in a single global window, unless `Window` is applied.

  For example, a simple form of windowing divides up the data into fixed-width time intervals, using `FixedWindows`.
  The following example demonstrates how to use `Window` in a pipeline that counts the number of occurrences of strings
  in each minute:

  ```
  TODO
  ```

  Let `{data, timestamp}` be a data element along with its timestamp. Then, if the input to this pipeline consists of

  ```
  [{"foo", 15s}, {"bar", 30s}, {"foo", 45s}, {"foo", 1m30s}]
  ```

  the output will be

  ```
  [{{"foo", 2}, 1m}, {{"bar", 1}, 1m}, {{"foo", 1}, 2m}]
  ```

  Several predefined `WindowFn`s are provided:

    * `Fixed` partitions the timestamps into fixed-width intervals.
    * `Sliding` places data into overlapping fixed-width intervals.
    * `Sessions` groups data into sessions where each item in a window is separated from the next by no more than a
      specified gap.

  Additionally, custom `WindowFn`s can be created, by implementing the `Dataflow.Transforms.Fns.WindowFn.Callable`
  protocol.

  ## Triggers

  `TODO` allows specifying a trigger to control when (in processing time) results for the given window can be produced.
  If unspecified, the default behaviour is to trigger first when the watermark passes the end of the window, and then
  trigger again every time there is late arriving data.

  Elements are added to the current window pane as they arrive. When the root trigger fires, output is produced based
  on the elements in the current pane.

  Depending on the trigger, this can be used both to output partial results early during the processing of the whole
  window, and to deal with late arriving in batches.

  Continuing the earlier example, if we wanted to emit the values that were available when the watermark passed the end
  of the window, and then output any late arriving element once-per (actual) hour until we have finished processing the
  next 24h of data (the use of watermark time to stop processing tends to be more robust if the data source is slow for
  a few days, etc.):

  ```
  TODO
  ```

  On the other hand, if we wanted to get early results every minute of processing time (for which there were new
  elements in the given window) we could do the following:

  ```
  TODO
  ```

  After a `GroupByKey` the trigger is set to a trigger that will preserve the intent of the upstream trigger. See
  `TODO.getContinuationTrigger/?` for more information.

  See `Trigger` for details on the available triggers.
  """

  use Dataflow.PTransform

  alias Dataflow.Window.WindowFn
  alias Dataflow.Trigger
  alias Dataflow.Window.WindowingStrategy

  defstruct [:fields]

  def new(opts \\ []) do
    unless Keyword.has_key?(opts, :into), do: raise "Must specify the `into:` parameter for windowing."
    into = Keyword.fetch!(opts, :into)
    window_fn = WindowFn.parse(into)

    fields = %{
      window_fn: window_fn
    }

    # triggering
    # discarding?
    # allowed lateness
    # output time Fn

    %__MODULE__{fields: fields}
  end

  defimpl PTransform.Callable do
    alias Dataflow.Transforms.Windowing.Window, as: WinXform
    alias Dataflow.Transforms.Core
    alias Dataflow.Transforms.Fns.DoFn
    alias Dataflow.Window.WindowFn.Callable, as: WindowFnC

    def expand(%WinXform{fields: fields}, input) do
      current_value = get_value(input)
      current_strategy = current_value.windowing_strategy

      strategy = struct!(current_strategy, fields)

      proxy_pvalue(input, from: input, windowing_strategy: strategy)
      ~> "PerformWindowing" -- Core.par_do(windowing_dofn(strategy.window_fn))
    end

    defp windowing_dofn(window_fn) do
      %DoFn{
        process: fn {el, timestamp, windows, opts} ->
          new_windows = WindowFnC.assign(window_fn, timestamp, el, windows)
          [{el, timestamp, new_windows, opts}] # todo check if this is totally valid
        end
      }
    end
  end

end
