defmodule Dataflow.DirectRunner.TransformEvaluator do
  @moduledoc """
  A behaviour which controls the evaluation of transforms in a direct pipeline.
  """

  @type state :: any
  @type transform :: Dataflow.PTransform.Callable.t
  @type element :: any

  @callback init(transform) :: {:ok, state} | {:error, any}
  @callback produce_element(state) :: {element, state}
  @callback produce_elements(pos_integer, state) :: {[element], state}
  @callback transform_element(state, element) :: {[element], state}
  @callback transform_elements(state, [element]) :: {[element], state}
  @callback consume_element(state, element) :: state
  @callback consume_elements(state, [element]) :: state
  @callback finish(state) :: :ok | {:error, any}

  @optional_callbacks \
    produce_element: 1, produce_elements: 2,
    transform_element: 2, transform_elements: 2,
    consume_element: 2, consume_elements: 2


  defmacro __using__(_opts) do
    quote do
      @behaviour unquote(__MODULE__)
    end
  end

  alias Dataflow.Transforms.{Core, IO}
  alias Dataflow.DirectRunner.TransformEvaluator

  def module_for(%Core.ParDo{}), do: TransformEvaluator.ParDo

  def module_for(%IO.ReadFile{}), do: TransformEvaluator.ReadFile
  def module_for(%IO.WriteFile{}), do: TransformEvaluator.WriteFile

  def module_for(%{__struct__: module}), do: raise "No evaluator available for transform #{module}"

end
