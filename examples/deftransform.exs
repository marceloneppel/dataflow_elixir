use Dataflow
alias Dataflow.Transforms.{Core, IO, Windowing, Aggregation}
alias Dataflow.DirectRunner

alias Dataflow.Utils.Time, as: DTime
require DTime

defmodule Test do

  deftransform extract_timestamps do
    defexpand(input, delay \\ 30) do
      input
      ~> Windowing.with_timestamps(&parse_as_timestamp(&1.created_at), delay_watermark: {delay, :seconds, :event_time})
    end

    defp parse_as_timestamp(string) do
      string
      |> Timex.parse!("{WDshort} {Mshort} {0D} {h24}:{0m}:{0s} {Z} {YYYY}")
      |> DateTime.to_unix
      |> DTime.timestamp(:seconds)
    end
  end

end
