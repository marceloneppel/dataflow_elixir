use Dataflow
alias Dataflow.Transforms.{Core, IO}
use IO.ReadFile
use IO.WriteFile
use Core.GroupByKey
use Core.CombineGlobally
alias Dataflow.Transforms.Fns.CombineFn
alias Dataflow.DirectRunner

require IEx
import Dataflow.Utils.PipelineVisualiser

p = Pipeline.new runner: DirectRunner

# Read the text file[pattern] into a PCollection.
lines =
  p
  ~> "read" -- read_file("examples/data/kinglear.txt")

# The regex used to extract words
word_regex = ~r/[A-Za-z']+/u

# Count the occurrences of each word.
counts =
  lines
  ~> "split" -- Core.flat_map(fn line -> Regex.scan(word_regex, line) |> List.flatten end)
  ~> "pair_with_one" -- Core.map(fn word -> {word, 1} end)
  ~> "group" -- group_by_key()
  ~> "count" -- Core.map(fn {word, ones} -> {word, Enum.sum(ones)} end)

# Format the counts into a PCollection of strings.
output =
  counts
  ~> "format" -- Core.map(fn {word, count} -> "#{word}: #{count}" end)

# Write the output using a "Write" transform that has side effects.
output
~> "write" -- write_file("examples/data/kinglear_words.txt")

# Actually run the pipeline, and await its result
# Pipeline.run p, sync: true

visualise p

# Command to run visualisation (fish shell)
# mix compile; and mix run examples/word_count.exs | dot -Tpng | display
