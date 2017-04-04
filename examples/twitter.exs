use Dataflow
alias Dataflow.Transforms.{Core, IO, Windowing, Aggregation}
use IO.ReadStream
use IO.WriteFile
use Core.GroupByKey
use Core.CombineGlobally
alias Dataflow.Transforms.Fns.CombineFn
alias Dataflow.DirectRunner

defp parse_as_timestamp(string), do: raise ""

autocomplete = Autocompleter.start_link

p = Pipeline.new runner: DirectRunner

p
~> IO.read_stream(ExTwitter.stream_sample())
~> Windowing.with_timestamps(fn tweet -> parse_as_timestamp(tweet.created_at) end, delay_watermark: {10, :seconds, :event_time})
~> Windowing.window_into(:sliding, size: {5, :minutes}, period: {2, :minutes})
~> Core.flat_map(fn tweet ->
  case tweet.entities[:hashtags] do
    nil -> []
    [] -> []
    list ->
      list
      |> Enum.map(fn %{text: text} -> String.downcase text end)
  end
 end)
~> Aggregation.count_per_key()
~> Core.flat_map(fn {tag, count} ->
  len = String.length tag
  for i <- 0..(len-1), prefix = String.slice(tag, 0..i), do: {prefix, {tag, count}}
 end)
~> Aggregation.top_per_key(compare: fn {_prefix, {_tag, count1}}, {_prefix, {_tag, count2}} -> count1 <= count2 end)
~> Core.map(fn {prefix, tcs} -> Enum.map tcs, fn {tag, _count} -> tag end end)
~> IO.send_to_process(autocomplete, mode: :batch)
