defmodule Dataflow.DirectRunner.TransformEvaluator.ParDo do
  alias Dataflow.Transforms.Core.ParDo
  alias Dataflow.Transforms.Fns.DoFn

  use Dataflow.DirectRunner.TransformEvaluator

  def init(%ParDo{do_fn: do_fn}) do
    # do something here to do with starting a bundle maybe. Not right now.
    {:ok, do_fn}
  end

  def process_element(element, %DoFn{process: process}) do
    # The process function should already be returning a list of elements, so no flattening needed
    process.(element)
  end

  def process_elements(elements, %DoFn{process: process}) do
    elements
    |> Enum.flat_map(process)
  end

  def finish(_state) do
    :ok
  end

end
