defmodule Dataflow.DirectRunner.TransformExecutor do
  alias Dataflow.Pipeline.AppliedTransform
  alias Dataflow.Utils
  alias Dataflow.PValue

  require Logger

  use GenStage

  alias Dataflow.Utils.Time
  require Time

  alias Dataflow.DirectRunner.TimingManager, as: TM

  defmodule InternalState do

    @type t :: %__MODULE__{
      mode: :producer | :consumer | :producer_consumer,
      applied_transform: Dataflow.Pipeline.AppliedTransform.t,
      callback_module: Dataflow.DirectRunner.TransformEvaluator.t,
      evaluator_state: any,
      timing_manager: pid
    }

    defstruct [:mode, :applied_transform, :callback_module, :evaluator_state, :timing_manager]

    def producer?(%__MODULE__{mode: :producer}), do: true
    def producer?(%__MODULE__{mode: :producer_consumer}), do: true
    def producer?(%__MODULE__{mode: :consumer}), do: false

    def consumer?(%__MODULE__{mode: :producer}), do: false
    def consumer?(%__MODULE__{mode: :prducer_consumer}), do: true
    def consumer?(%__MODULE__{mode: :consumer}), do: true
  end

  # Public API

  def notify_of_timers(pid, timers) do
    GenStage.cast pid, {:timers, timers}
  end

  def notify_downstream_of_advanced_owm(pid, new_wm) do
    GenStage.cast pid, {:advance_owm, new_wm}
  end

  def notify_evaluator(pid, message) do
    GenStage.cast pid, {:evaluator, message}
  end


  #TODO change this once the tree is dynamic
  defp via_transform_registry(transform_id) do
    {:via, Registry, {Dataflow.DirectRunner.TransformRegistry, transform_id}}
  end

  defp get_value(value_id) do
    Dataflow.DirectRunner.ValueStore.get(value_id)
  end

  defp get_producer(value) do
    case value do
      %{producer: {:proxy, from}} -> get_producer(get_value(from))
      %{producer: producer} -> producer
    end
  end

  def start_link(%AppliedTransform{id: id} = transform) do
    GenStage.start_link(__MODULE__, transform, name: via_transform_registry(id))
  end

  def init(%AppliedTransform{id: _id, transform: transform, input: input_id, output: output_id} = at) do
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
        [subscribe_to: [via_transform_registry(get_producer(input))]]
      end

    evaluator_module = Dataflow.DirectRunner.TransformEvaluator.module_for transform
    tm_opts = evaluator_module.timing_manager_options

    {:ok, timing_manager} = TM.start_linked(tm_opts)

    Logger.debug "Options: #{inspect opts}\nTiming opts: #{inspect tm_opts}"

    {:ok, evaluator_state} = evaluator_module.init(transform, input, timing_manager)


    Logger.debug "Started with mode #{mode}."

    state = %InternalState{
      mode: mode,
      applied_transform: at,
      callback_module: evaluator_module,
      evaluator_state: evaluator_state,
      timing_manager: timing_manager
    }

    {mode, state, opts}
  end

  def handle_demand(demand, %InternalState{mode: :producer, applied_transform: at, callback_module: module, evaluator_state: state} = ex_state) do
    Logger.debug fn -> "#{transform_label(at)}: demand of #{demand} received." end
    {results, new_state} = module.produce_elements(demand, state) # todo allow demand buffering
    new_int_state = %{ex_state | evaluator_state: new_state}
    {:noreply, results, new_int_state}
  end

  def handle_events(elements, _from, %InternalState{mode: mode, callback_module: module, evaluator_state: state} = ex_state) do
    {results, new_state} = module.transform_elements(elements, state)
    {:noreply, maybe_results(results, ex_state), %{ex_state | evaluator_state: new_state}}
  end

  def handle_info({_from, {:watermark, new_watermark}}, ex_state) do
    TM.advance_input_watermark(ex_state.timing_manager, new_watermark)

    {:noreply, [], ex_state}
  end

  def handle_cast({:timers, timers}, %InternalState{callback_module: module, evaluator_state: state} = ex_state) do
    {elements, new_state} = module.fire_timers timers, state

    Logger.debug fn -> "#{transform_label(ex_state.applied_transform)}: I received timers: #{inspect timers} and on firing they produced #{Enum.count elements} elements." end

    {:noreply, maybe_results(elements, ex_state), %{ex_state | evaluator_state: new_state}}
  end

  def handle_cast({:advance_owm, new_owm}, %InternalState{mode: mode} = ex_state) do
#    Logger.debug fn -> "#{transform_label(ex_state.applied_transform)}: advancing output watermark to #{inspect new_owm}" end
    unless mode == :consumer, do: GenStage.async_notify(self(), {:watermark, new_owm})
    log_owm(ex_state.applied_transform.id, new_owm)
    {:noreply, [], ex_state}
  end

  def handle_cast({:evaluator, message}, %InternalState{callback_module: module, evaluator_state: state} = ex_state) do
    {elements, new_state} = module.handle_async(message, state)

    Logger.debug fn -> "#{transform_label(ex_state.applied_transform)}: I received an evaluator async message and on processing it produced #{Enum.count elements} elements." end

    {:noreply, maybe_results(elements, ex_state), %{ex_state | evaluator_state: new_state}}
  end

  defp transform_label(at) do
    "<#{Utils.make_transform_label at, newline: false}>"
  end

  defp maybe_results(_, %InternalState{mode: :consumer}), do: []
  defp maybe_results(results, _), do: results

  defp log_owm(id, owm) do
    # todo take config

    raw = owm |> Time.raw
    Dataflow.DirectRunner.StatsCollector.log_output_watermark(id, raw)
  end
end
