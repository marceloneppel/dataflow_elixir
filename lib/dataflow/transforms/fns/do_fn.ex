defmodule Dataflow.Transforms.Fns.DoFn do
  @moduledoc """
  A structure and behaviour encapsulating the parameters to a transform with custom processing, e.g. a `ParDo`. Essentially a map operation with custom start/end bundle logic.
  """

  #todo typespec

  defstruct \
    start_bundle: &__MODULE__._default_bundle_process/1,
    end_bundle: &__MODULE__._default_bundle_process/1,
    process: nil

  def _default_bundle_process(x), do: x



  #todo behaviour

  def from_function(fun) when is_function(fun, 1) do
    %__MODULE__{
      process: fn {el, timestamp, windows, opts} ->
        fun.(el)
        |> Enum.map(&{&1, timestamp, windows, opts})
      end
    }
  end
end
