defmodule Dataflow.Transforms.Windowing.WithTimestamps do
  use Dataflow.PTransform
  alias Dataflow.Utils

  defstruct [:timestamp_fn, :opts]

  def new(timestamp_fn, opts \\ []) do
    Utils.verify_opts!(opts, [:delay_watermark, :no_watermark])

    if Keyword.has_key?(opts, :delay_watermark) && Keyword.has_key?(opts, :no_watermark) do
      raise "Can only specify one of `delay_watermark` and `no_watermark`."
    end

    # TODO more validation

    %__MODULE__{timestamp_fn: timestamp_fn, opts: opts}
  end

  defimpl PTransform.Callable do
    alias Dataflow.Transforms.Windowing.WithTimestamps
    alias Dataflow.Transforms.Windowing

    def expand(%WithTimestamps{timestamp_fn: timestamp_fn, opts: opts}, input) do
      input
      ~> Windowing.assign_timestamps(timestamp_fn)
      |> maybe_watermark(opts[:no_watermark], opts[:delay_watermark])
    end

    defp maybe_watermark(input, true, _), do: input
    defp maybe_watermark(input, _, delay) do
      opts = maybe_delay(delay)
      input
      ~> Windowing.watermark(opts)
    end


    defp maybe_delay(nil), do: []
    defp maybe_delay(delay), do: [delay: delay]
  end
end
