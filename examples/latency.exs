use Dataflow
alias Dataflow.Transforms.{Core, IO, Windowing}
alias Dataflow.DirectRunner
alias Dataflow.Transforms.Fns.DoFn

alias Dataflow.Utils.Time, as: DTime
require DTime



options = [stream_delay: :integer, num_extra_transforms: :integer, log_prefix: :string]

{args, _, _} = OptionParser.parse(System.argv(), switches: options)

stream_delay = Keyword.fetch!(args, :stream_delay)
number_extra_transforms = Keyword.fetch!(args, :num_extra_transforms)
log_prefix = Keyword.fetch!(args, :log_prefix)

Application.put_env(:dataflow_elixir, :stats_prefix, log_prefix)

case Keyword.get(args, :log_path) do
  nil -> :ok
  path -> Application.put_env(:dataflow_elixir, :stats_path, path)
end

# generate an infinite stream of increasing integers, emitting a max of 1 per millisecond,
# and tag each one with the time it was produced
mk_stream = fn ->
  Stream.interval(stream_delay)
  |> Stream.map(fn num ->
    time =
      System.os_time(:microseconds)
      |> DTime.timestamp(:microseconds)

    {num, time, [:global], []}
   end)
end

log_timestamp_dofn = DoFn.from_function(
  fn _el, time, _windows, _opts ->
    DirectRunner.StatsCollector.log_output_watermark("out", DTime.raw(time))
    []
  end
)



p = Pipeline.new runner: DirectRunner

source =
  p
  ~> IO.read_stream(mk_stream, chunk_size: 1, full_elements?: true)

# apply primitive transforms
transformed =
  Enum.reduce(1..number_extra_transforms, source, fn _, collection ->
    collection ~> Core.map(fn x -> x + 1 end)
   end)

# calculate the latency in getting through the system
# finally print out the numbers to the console (or not, since this is a bottleneck)
transformed
~> Core.par_do(log_timestamp_dofn)

Pipeline.run p, sync: true
