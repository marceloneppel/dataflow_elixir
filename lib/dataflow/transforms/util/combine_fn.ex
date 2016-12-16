defmodule Dataflow.Transforms.Util.CombineFn do
  @moduledoc """
  A CombineFn specifies how multiple values in all or part of a PCollection can
  be merged into a single value---essentially providing the same kind of
  information as the arguments to a reduce operation. The
  combining process proceeds as follows:

  1. Input values are partitioned into one or more batches.
  2. For each batch, the create_accumulator method is invoked to create a fresh
     initial "accumulator" value representing the combination of zero values.
  3. For each input value in the batch, the add_input method is invoked to
     combine more values with the accumulator for that batch.
  4. The merge_accumulators method is invoked to combine accumulators from
     separate batches into a single combined output accumulator value, once all
     of the accumulators have had all the input value in their batches added to
     them. This operation is invoked repeatedly, until there is only one
     accumulator value left.
  5. The extract_output operation is invoked on the final accumulator to get
     the output value.
  """

  @type el :: any
  @type acc :: any
  @type t :: %__MODULE__{
    create_accumulator: (() -> acc),
    add_input: (acc, el -> acc),
    merge_accumulators: (acc, acc -> acc),
    extract_output: (acc -> any)
  }

  defstruct \
    create_accumulator: nil,
    add_input: nil,
    # skip add inputs for now
    merge_accumulators: nil,
    extract_output: &__MODULE__._default_extract_output/1

  def _default_extract_output(acc), do: acc


  #todo behaviour

  def from_functions(create, acc, merge) when is_function(create, 1) and is_function(acc, 2) and is_function(merge, 2) do
    %__MODULE__{create_accumulator: create, add_input: acc, merge_accumulators: merge}
  end

   #todo from single callable?
end
