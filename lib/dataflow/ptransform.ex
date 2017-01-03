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
          import unquote(__MODULE__), only: [fresh_pvalue: 1]
          import Dataflow, only: [~>: 2]
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

  def fresh_pvalue(%NestedInput{state: state}) do
    #todo LABEL????

    %PValue{
      id: NestedState.fresh_id(state),
      #label: ???,
      producer: NestedState.peek_context(state)
    }
  end

  def apply(transform, nested_input) do
      Dataflow.PTransform.Callable.apply transform, nested_input
  end

  defprotocol Callable do
    @spec apply(transform :: struct, input :: Dataflow.Pipeline.NestedState.t) :: Dataflow.PValue.value
    def apply(transform, input)
  end
end
