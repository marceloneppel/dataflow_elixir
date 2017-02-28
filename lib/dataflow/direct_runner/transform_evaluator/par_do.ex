defmodule Dataflow.DirectRunner.TransformEvaluator.ParDo do
  alias Dataflow.Transforms.Core.ParDo
  alias Dataflow.Transforms.Fns.DoFn

  use Dataflow.DirectRunner.TransformEvaluator

  def init(%ParDo{do_fn: do_fn}, _input) do
    # do something here to do with starting a bundle maybe. Not right now.
    {:ok, :no_update, do_fn}
  end

  def transform_element(element, %DoFn{process: process} = state) do
    # The process function should already be returning a list of elements, so no flattening needed
    {process.(element), :no_update, state}
  end

  def transform_elements(elements, %DoFn{process: process} = state) do
    {Enum.flat_map(elements, process), :no_update, state}
  end

  def finish(_state) do
    :ok
  end

end
