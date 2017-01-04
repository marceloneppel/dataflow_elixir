use Dataflow
alias Dataflow.Transforms.Core
use Core.Create
use Core.GroupByKey
use Core.CombineGlobally
alias Dataflow.Transforms.Fns.CombineFn

require IEx
import Dataflow.Utils.PipelineVisualiser

p = Pipeline.new

p
~> create([{1,2}, {1,3}, {2,3}])
~> group_by_key()
~> "funky label" -- Core.flat_map(fn x -> [x, x] end)

p
~> create([{5,6},{5,6},{5,6},{5,6}])
~> "funky label" -- Core.flat_map(fn x -> [x, x] end)
~> "funky label 22" -- Core.map(fn x -> x end)
~> combine_globally(%CombineFn{})

visualise p

# Command to run visualisation (fish shell)
# mix compile; and mix run examples/basic.exs | dot -Tpng | display
