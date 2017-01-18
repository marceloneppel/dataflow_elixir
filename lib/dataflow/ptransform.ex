defmodule Dataflow.PTransform do
  @moduledoc """

  """

  alias Dataflow.Pipeline.{NestedInput, NestedState}
  alias Dataflow.PValue

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

    type = Keyword.get opts, :type, :normal

    value =
      %PValue{
        id: NestedState.fresh_id(state),
        #label: ???,
        producer: NestedState.peek_context(state),
        pipeline: NestedState.pipeline(state),
        type: type
      }

    %NestedInput{state: state, value: value}
  end

  def expand(transform, nested_input) do
      Dataflow.PTransform.Callable.expand transform, nested_input
  end

  defprotocol Callable do
    @spec expand(transform :: t, input :: Dataflow.Pipeline.NestedState.t) :: Dataflow.PValue.value
    def expand(transform, input)
  end
end
