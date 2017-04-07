defmodule Dataflow.Transforms.Windowing.AssignTimestamps do
  use Dataflow.PTransform

  defstruct [:timestamp_fn]

  def new(timestamp_fn) when is_function(timestamp_fn), do: %__MODULE__{timestamp_fn: timestamp_fn}

  defimpl PTransform.Callable do
    alias Dataflow.Transforms.Windowing.AssignTimestamps
    alias Dataflow.Transforms.Core
    alias Dataflow.Transforms.Fns.DoFn

    def expand(%AssignTimestamps{timestamp_fn: timestamp_fn}, input) do
      input
      ~> Core.par_do(assign_dofn(timestamp_fn))
    end

    defp assign_dofn(timestamp_fn) do
      %DoFn{
        process: fn {el, _timestamp, windows, opts} ->
          # TODO check assumptions here.
          # Beam implementation keeps existing windows
          # Assuming that opts can stay the same (but e.g. pane data may be invalid? not with keeping windows the same however)

          new_timestamp = timestamp_fn.(el)
          [{el, new_timestamp, windows, opts}]
        end
      }
    end
  end
end
