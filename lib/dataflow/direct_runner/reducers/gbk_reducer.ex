defmodule Dataflow.DirectRunner.Reducers.GBKReducer do
  @behaviour Dataflow.DirectRunner.ReducingEvaluator.Reducer

  alias Dataflow.Transforms.Core.GroupByKey

  def init(%GroupByKey{}) do
    []
  end

  def process_value(value, _context, _attrs, acc) do
    [value | acc]
  end

  def merge_accumulators(accs, _context) do
    Enum.concat accs
  end

  def emit(_pane_info, _context, acc) do
    {[acc], acc}
  end

  def clear_accumulator(_acc) do
    []
  end
end
