defmodule Dataflow.DirectRunner.TransformExecutor do
  alias Dataflow.Pipeline.AppliedTransform
  alias Dataflow.Utils
  alias Dataflow.PValue
  alias Dataflow.DirectRunner.ExecutableTransform

  require Logger

  use GenStage

  alias Dataflow.Utils.Time



  defmodule InternalState do

    @type t :: %__MODULE__{
      mode: :producer | :consumer | :producer_consumer,
      applied_transform: Dataflow.Pipeline.AppliedTransform.t,
      callback_module: Dataflow.DirectRunner.TransformEvaluator.t,
      evaluator_state: any,
      local_input_wm: Time.timestamp, # nil for producers
      local_output_wm: Time.timestamp | nil, # nil for consumers
    }

    #TODO specialised state for watermarks?

    defstruct [:mode, :applied_transform, :callback_module, :evaluator_state, :local_input_wm, :local_output_wm]

    def producer?(%__MODULE__{mode: :producer}), do: true
    def producer?(%__MODULE__{mode: :producer_consumer}), do: true
    def producer?(%__MODULE__{mode: :consumer}), do: false

    def consumer?(%__MODULE__{mode: :producer}), do: false
    def consumer?(%__MODULE__{mode: :prducer_consumer}), do: true
    def consumer?(%__MODULE__{mode: :consumer}), do: true
  end

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

  def init(%AppliedTransform{id: id, transform: transform, input: input_id, output: output_id} = at) do
    Logger.info fn -> "Starting stage #{inspect self()} for transform #{transform_label(at)}..." end

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
        [subscribe_to: [via_transform_registry(input.producer)]]
      end

    Logger.debug "Options: #{inspect opts}"

    evaluator_module = Dataflow.DirectRunner.TransformEvaluator.module_for transform
    {:ok, evaluator_state} = evaluator_module.init(transform)

    Logger.debug "Started with mode #{mode}."

    state = %InternalState{
      mode: mode,
      applied_transform: at,
      callback_module: evaluator_module,
      evaluator_state: evaluator_state,
      local_input_wm: Time.min_timestamp
    }

    {mode, state, opts}
  end

  def handle_demand(demand, %InternalState{mode: :producer, applied_transform: at, callback_module: module, evaluator_state: state} = ex_state) do
    Logger.debug fn -> "#{transform_label(at)}: demand of #{demand} received." end
    {status, results, new_state} = module.produce_elements(demand, state)
    new_int_state = %{ex_state | evaluator_state: new_state}
    case status do
      :active -> {:noreply, results, new_int_state}
      :finished ->
        new_int_state = advance_output_watermark(Time.max_timestamp, new_int_state)
        {:noreply, results, new_int_state} #TODO! Stop?? Finish?
    end
  end

  defp advance_output_watermark(watermark, %InternalState{applied_transform: at, local_output_wm: lowm} = state) do
    case watermark do
      ^lowm ->
        Logger.debug fn -> "#{transform_label(at)}: no change in watermark" end
        state
      new_watermark ->
        Logger.debug fn -> "#{transform_label(at)}: advancing output watermark to #{inspect watermark}" end
        GenStage.async_notify(self, {:watermark, new_watermark})
        %{state | local_output_wm: watermark}
    end

  end

  def handle_events(elements, _from, %InternalState{mode: :producer_consumer, applied_transform: at, callback_module: module, evaluator_state: state} = ex_state) do
    {results, new_state} = module.transform_elements(elements, state)
    {:noreply, results, %{ex_state | evaluator_state: new_state}}
  end

  def handle_events(elements, _from, %InternalState{mode: :consumer, applied_transform: at, callback_module: module, evaluator_state: state} = ex_state) do
    new_state = module.consume_elements(elements, state)
    {:noreply, [], %{ex_state | evaluator_state: new_state}}
  end

  def handle_info({_from, {:watermark, new_watermark}}, %InternalState{callback_module: module, evaluator_state: state, local_input_wm: liwm} = ex_state) do
    # notify internal execution module that watermark has changed
    {output_watermark, events, new_ev_state} = module.update_input_watermark new_watermark, state # triggers etc are in here

    Logger.debug fn -> "#{transform_label(ex_state.applied_transform)}: I received an input watermark of #{inspect new_watermark} and my new output watermark is #{inspect output_watermark}." end

    new_state = advance_output_watermark(output_watermark, %{ex_state | evaluator_state: new_ev_state})

    {:noreply, events, new_state}
  end

  defp transform_label(at) do
    "<#{Utils.make_transform_label at, newline: false}>"
  end
end
