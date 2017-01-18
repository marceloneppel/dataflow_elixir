defmodule Dataflow.DirectRunner.TransformExecutor do
  alias Dataflow.Pipeline.AppliedTransform
  alias Dataflow.Utils
  alias Dataflow.PValue
  alias Dataflow.DirectRunner.ExecutableTransform

  require Logger

  use GenStage

  #TODO change this once the tree is dynamic
  defp via_transform_registry(transform_id) do
    {:via, Registry, {Dataflow.DirectRunner.TransformRegistry, transform_id}}
  end

  defp get_value(value_id) do
    Dataflow.DirectRunner.ValueStore.get(value_id)
  end

  def start_link(%AppliedTransform{id: id} = transform) do
    GenStage.start_link(__MODULE__, transform, name: via_transform_registry(id))
  end

  def init(%AppliedTransform{transform: transform, input: input_id, output: output_id} = at) do
    Logger.info fn -> "Starting stage for transform #{Utils.make_transform_label at, newline: false}..." end

    input = get_value(input_id)
    output = get_value(output_id)

    producer? = not PValue.dummy? output
    consumer? = not PValue.dummy? input

    mode =
      cond do
        producer? && consumer? -> :producer_consumer
        producer? -> :producer
        consumer? -> :consumer
        true -> raise "Both input and output are dummies, which is invalid."
      end


    opts =
      if PValue.dummy? input do
        []
      else
        [subscribe_to: [via_transform_registry(input_id)]]
      end

    evaluator_module = Dataflow.DirectRunner.TransformEvaluator.module_for transform
    {:ok, evaluator_state} = evaluator_module.init(transform)

    Logger.info "Started with mode #{mode}."

    {mode, {producer?, consumer?, at, evaluator_module, evaluator_state}, opts} # store the applied transform as our state
  end

  def handle_demand(demand, {true, false, at, module, state}) do
    case module.produce_elements(demand, state) do
      {:active, results, new_state} -> {:noreply, results, {true, false, at, module, new_state}}
      {:finished, results, new_state} -> {:noreply, results, {true, false, at, module, new_state}} #TODO! Stop?? Finish?
    end
  end

  def handle_events(elements, _from, {true, true, at, module, state}) do
    {results, new_state} = module.transform_elements(elements, state)
    {:noreply, results, {true, true, at, module, new_state}}
  end

  def handle_events(elements, _from, {false, true, at, module, state}) do
    new_state = module.consume_elements(elements, state)
    {:noreply, [], {false, true, at, module, new_state}}
  end
end
