defmodule Dataflow.DirectRunner.Reducers.CombineReducer do
  @behaviour Dataflow.DirectRunner.ReducingEvaluator.Reducer

  alias Dataflow.Transforms.Core.CombinePerKey
  alias Dataflow.Transforms.Fns.CombineFn

  def init(%CombinePerKey{combine_fn: cfn}) do
    CombineFn.p_create_accumulator(cfn) # keeps the accumulator and original function around
  end

  def process_value(value, _context, _attrs, acc) do
    CombineFn.p_add_input(acc, value)
  end

  def merge_accumulators(accs, _context) do
    case accs do
      [] -> raise "Cannot merge a list of empty accumulators"
      list when is_list(list) ->
        Enum.reduce accs, &CombineFn.p_merge_accumulators/2 # pattern match ensures the cfns are the same
    end
  end

  def emit(_pane_info, _context, acc) do
    output = CombineFn.p_extract_output(acc)
    {[output], acc}
  end

  def clear_accumulator(acc) do
    CombineFn.p_clear_accumulator(acc)
  end
end
