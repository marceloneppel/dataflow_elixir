use Dataflow
alias Dataflow.Transforms.{Core, IO}
alias Dataflow.DirectRunner

stream_delay = 100
number_extra_transforms = 10000

# generate an infinite stream of increasing integers, emitting a max of 1 per millisecond,
# and tag each one with the time it was produced
mk_stream = fn ->
  numbers = Stream.interval(stream_delay)
  times = Stream.repeatedly(&System.os_time/0)

  Stream.zip(numbers, times)
end

p = Pipeline.new runner: DirectRunner

source = p ~> IO.read_stream(mk_stream, 1)

# apply identity transforms
transformed =
  Enum.reduce(1..number_extra_transforms, source, fn _, collection ->
    collection ~> Core.map(fn x -> x end)
   end)

# calculate the latency in getting through the system
# finally print out the numbers to the console (or not, since this is a bottleneck)
transformed
~> Core.each(fn {_, time} -> DirectRunner.StatsCollector.log_output_watermark("out", time) end)

Pipeline.run p, sync: true
