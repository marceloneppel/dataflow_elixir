defmodule Dataflow.DirectRunner.TransformEvaluator do
  @moduledoc """
  A behaviour which controls the evaluation of transforms in a direct pipeline.
  """

  alias Dataflow.Utils.Time

  @type t :: module

  @type state :: any
  @type transform :: Dataflow.PTransform.Callable.t
  @type element :: {
      value :: any | {any, any}, # non-keyed or keyed
      timestamp :: Time.timestamp,
      windows :: [Dataflow.Window.t]
      #,options :: keyword ???
    }

  @callback init(transform) :: {:ok, state} | {:error, any}

  @callback produce_elements(pos_integer, state) :: {(:active | :finished), [element], state}

  @callback transform_element(element, state) :: {[element], state}
  @callback transform_elements([element], state) :: {[element], state}

  @callback consume_element(element, state) :: state
  @callback consume_elements([element], state) :: state

  @callback update_input_watermark(Time.timestamp, state) :: {Time.timestamp, [element], state}

  @callback finish(state) :: :ok | {:error, any}

  @optional_callbacks \
    produce_elements: 2,
    transform_element: 2, transform_elements: 2,
    consume_element: 2, consume_elements: 2


  defmacro __using__(opts) do
    blocks = [
      quote do
        @behaviour unquote(__MODULE__)
      end
    ]
    ++
    case opts[:type] do
      :reducing -> [] # require the user to implement this
      t when t == nil or t == :elementwise -> [
        quote do
          def update_input_watermark(new_watermark, state), do: {new_watermark, [], state}
        end
      ]
    end

    blocks
  end

  alias Dataflow.Transforms.{Core, IO}
  alias Dataflow.DirectRunner.TransformEvaluator

  def module_for(%Core.ParDo{}), do: TransformEvaluator.ParDo
  def module_for(%Core.GroupByKey{}), do: TransformEvaluator.GroupByKey

  def module_for(%IO.ReadFile{}), do: TransformEvaluator.ReadFile
  def module_for(%IO.WriteFile{}), do: TransformEvaluator.WriteFile


  def module_for(%{__struct__: module}), do: raise "No evaluator available for transform #{module}"

end
