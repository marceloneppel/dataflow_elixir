defmodule Dataflow.PTransform do
  @moduledoc """

  """

  alias Dataflow.Pipeline.{NestedInput, NestedState}
  alias Dataflow.PValue
  require Dataflow.Pipeline
  require NestedInput

  defmacro __using__(opts) do
    code =
      [
        quote do
          alias unquote(__MODULE__)
          import unquote(__MODULE__), only: [fresh_pvalue: 1, fresh_pvalue: 2]
          use Dataflow
        end
      ]

    using =
      case Keyword.get opts, :make_fun do
        nil -> []
        funs when is_list(funs) ->
          [
            quote do
              defmacro __using__(_opts) do
                funs = unquote(funs)
                quote do
                  import unquote(__MODULE__), only: unquote(funs)
                end
              end
            end
          ]
      end

    quote do unquote_splicing(code ++ using) end
  end

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

    %NestedInput{state: state, value: value}
  end

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
