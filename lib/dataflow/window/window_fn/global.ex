defmodule Dataflow.Window.WindowFn.Global do
  @moduledoc """
  A windowing function that assigns everything to one global window.
  """

  alias Dataflow.Window.WindowFn

  defstruct []

  def new, do: %__MODULE__{}

  defimpl WindowFn.Callable do
    use WindowFn

    def non_merging?(_), do: true

    def assign(_, _timestamp, _element, _windows) do
      [Dataflow.Window.global]
    end

    def side_input_window(_, _window) do
      Dataflow.Window.global
    end
  end

end
