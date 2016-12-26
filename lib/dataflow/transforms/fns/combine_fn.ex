defmodule Dataflow.Transforms.Fns.CombineFn do
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

  @callback create_accumulator() :: acc
  @callback add_input(acc :: acc, el :: el) :: acc
  @callback merge_accumulators(acc1 :: acc, acc2 :: acc) :: acc
  @callback extract_output(acc :: acc) :: any

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

   # These operate in tuples so that they're easily pipeable.

   def create_accumulator(%__MODULE__{create_accumulator: ca} = function) do
     {function, ca.()}
   end

   def add_input({%__MODULE__{add_input: ai} = function, acc}, input) do
     {function, ai.(acc, input)}
   end

   #todo make this only be the default impl?
   def add_inputs({%__MODULE__{} = function, acc}, inputs) do
     Enum.reduce(inputs, acc, fn el, acc -> add_input {function, acc}, el end)
   end

   def merge_accumulators({%__MODULE__{merge_accumulators: ma} = function, acc1}, acc2) do
     {function, ma.(acc1, acc2)}
   end

   def extract_output({%__MODULE__{extract_output: eo}, acc}) do
     eo.(acc)
   end
end
