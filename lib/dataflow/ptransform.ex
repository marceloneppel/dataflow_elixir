defmodule Dataflow.PTransform do
  @moduledoc """

  """

  alias Dataflow.Pipeline.{NestedInput, NestedState}
  alias Dataflow.PValue
  require Dataflow.Pipeline
  require NestedInput

  defmacro __using__(opts) do
    quote do
      alias unquote(__MODULE__)
      import unquote(__MODULE__), only: [fresh_pvalue: 1, fresh_pvalue: 2, get_value: 1, proxy_pvalue: 1, proxy_pvalue: 2]
      use Dataflow
    end
  end

  import Apex.AwesomeDef

  def fresh_pvalue(%NestedInput{state: state}, opts \\ []) do
    #todo LABEL????
    value = get_from_value(opts[:from])
    id = NestedState.fresh_id(state)
    value =
      %{value |
        id: id,
        label: opts[:label] || "##{id}",
        producer: NestedState.peek_context(state),
        pipeline: NestedState.pipeline(state),
        type: opts[:type] || :collection
      }

    value =
      case opts[:windowing_strategy] do
        nil -> value
        strategy -> %{value | windowing_strategy: strategy}
      end

    %NestedInput{state: state, value: value}
  end

  @doc "Provides an input which contains a proxy value, that is one which only changes PValue options."
  def proxy_pvalue(%NestedInput{value: from_value} = input, opts \\ []) do
    %NestedInput{state: state, value: new_value} = fresh_pvalue(input, opts)
    new_value =
      %{new_value |
        producer: {:proxy, from_value.id},
        type: :proxy
      }

    # register this value
    NestedState.add_value(state, new_value)

    %NestedInput{state: state, value: new_value}
  end

  def get_value(input), do: get_from_value(input)

  defp get_from_value(%PValue{} = value), do: value
  defp get_from_value(%NestedInput{value: value}), do: value
  defp get_from_value(nil), do: %PValue{}

  def expand(transform, nested_input) do
      Dataflow.PTransform.Callable.expand transform, nested_input
  end

  defprotocol Callable do
    @spec expand(transform :: t, input :: Dataflow.Pipeline.NestedState.t) :: Dataflow.PValue.value
    def expand(transform, input)
  end
end
