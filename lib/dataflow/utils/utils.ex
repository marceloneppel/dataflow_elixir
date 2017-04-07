defmodule Dataflow.Utils do

  def mod(x, y) when x > 0, do: rem(x, y)
  def mod(x, y) when x < 0, do: y + rem(x, y)
  def mod(0, _y), do: 0

  # transform utils

  alias Dataflow.Pipeline.AppliedTransform

  def make_transform_label(_transform, _opts \\ [])

  def make_transform_label(%AppliedTransform{label: l, transform: transform, id: id}, _opts) when l == nil or l == "" do
    "##{id} {#{get_label_from_transform(transform)}}"
  end

  def make_transform_label(%AppliedTransform{id: id, label: label, transform: transform}, opts) do
    separator = if (Keyword.get opts, :newline, true), do: "\n", else: " "
    "##{id} #{label}#{separator}{#{get_label_from_transform(transform)}}"
  end

  defp get_label_from_transform(%{__struct__: module}) do
    module
    |> Atom.to_string
    |> String.replace_leading("Elixir.Dataflow.Transforms.", "")
  end

  def verify_opts!(opts, allowed) do
    unless Keyword.keyword? opts do
      raise "Must pass in a keyword list"
    end

    Enum.each opts, fn {key, _} ->
      cond do
        key in allowed -> :ok
        true -> raise "#{key} is an unrecognised option"
      end
    end
  end
end
