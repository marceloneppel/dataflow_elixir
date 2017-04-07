defmodule Dataflow.DirectRunner.TransformEvaluator do
  @moduledoc """
  A behaviour which controls the evaluation of transforms in a direct pipeline.
  """

  alias Dataflow.Utils.Time

  @type t :: module

  @type state :: any
  @type transform :: Dataflow.PTransform.Callable.t
  @type pvalue :: Dataflow.PValue.t
  @type element :: {
    value :: any | {any, any}, # non-keyed or keyed
    timestamp :: Time.timestamp,
    windows :: [Dataflow.Window.t],
    options :: keyword
  }

  @type timer :: {
    namespace :: {Window.t, any},
    time :: Time.timestamp,
    domain :: :event_time
  }

  @callback init(transform, pvalue, timing_manager :: pid) :: {:ok, state} | {:error, any}

  @callback produce_elements(pos_integer, state) :: {[element], state}

  @callback transform_element(element, state) :: {[element], state}
  @callback transform_elements([element], state) :: {[element], state}

  @callback consume_element(element, state) :: state
  @callback consume_elements([element], state) :: state

  @callback fire_timers([timer], state) :: {[element], state}

  @callback handle_async(message :: any, state) :: {[element], state}

  @callback finish(state) :: :ok | {:error, any}

  @callback timing_manager_options :: keyword

  @optional_callbacks \
    produce_elements: 2,
    transform_element: 2, transform_elements: 2,
    consume_element: 2, consume_elements: 2,
    fire_timers: 2,
    handle_async: 2


  defmacro __using__(opts) do
    quote do
      @behaviour unquote(__MODULE__)

      def timing_manager_options, do: []

      defoverridable timing_manager_options: 0
    end
  end

  alias Dataflow.Transforms.{Core, IO, Windowing}
  alias Dataflow.DirectRunner.{TransformEvaluator, ReducingEvaluator}

  def module_for(%Core.ParDo{}), do: TransformEvaluator.ParDo
  def module_for(%Core.GroupByKey{}), do: ReducingEvaluator
  def module_for(%Core.CombinePerKey{}), do: ReducingEvaluator

  def module_for(%IO.ReadFile{}), do: TransformEvaluator.ReadFile
  def module_for(%IO.WriteFile{}), do: TransformEvaluator.WriteFile
  def module_for(%IO.ReadStream{}), do: TransformEvaluator.ReadStream

  def module_for(%Windowing.Watermark{}), do: TransformEvaluator.Watermark


  def module_for(%{__struct__: module}), do: raise "No evaluator available for transform #{module}"

end
