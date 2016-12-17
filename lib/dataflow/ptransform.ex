defmodule Dataflow.PTransform do
  @moduledoc """

  """

  alias Dataflow.Pipeline.{NestedInput, NestedState}
  alias Dataflow.PValue

  @callback apply(transform :: struct, input :: Dataflow.Pipeline.NestedState.t) :: Dataflow.PValue.value

  defmacro __using__(opts) do
    code =
      [
        quote do
          @behaviour unquote(__MODULE__)
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

    derive =
      case Keyword.get opts, :no_protocol, false do
        true -> []
        false ->
          [
            quote do
              defimpl Dataflow.PTransform.Protocol do
                def apply(data, input) do
                  __MODULE__.apply(data, input)
                end
              end
            end
          ]
        _ -> raise "`no_protocol` options must be `true` or `false`"
      end

    quote do unquote_splicing(code ++ using ++ derive) end
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
      Dataflow.PTransform.Protocol.apply transform, nested_input
  end

end
