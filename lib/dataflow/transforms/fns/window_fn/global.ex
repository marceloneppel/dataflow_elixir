defmodule Dataflow.Transforms.Fns.WindowFn.Global do
  @moduledoc """
  A windowing function that assigns everything to one global window.
  """

  use Dataflow.Transforms.Fns.WindowFn

  def non_merging?(_), do: true

  def assign(_, _timestamp, _element, _windows) do
    [Dataflow.Window.global]
  end

  def side_input_window(_, _window) do
    Dataflow.Window.global
  end

  def new, do: %__MODULE__{}
end
