use Dataflow
alias Dataflow.Transforms.Core
use Core.Create
use Core.GroupByKey
require IEx

Pipeline.new
~> create([{1,2}, {1,3}, {2,3}])
~> group_by_key()

IEx.pry
