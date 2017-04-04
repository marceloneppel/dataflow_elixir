defmodule Dataflow.Utils.PriorityQueue do
  @moduledoc """
  Extremely simple, inefficient priority queue using a naive list implementation.

  Should be replaced by something more efficient.
  """

  def new(comparator \\ &Kernel.<=/2), do: {[], comparator}

  def put({queue, cmp}, key, val), do: {put(queue, key, val, [], cmp), cmp}

  defp put([], key, val, rest, _cmp) do
    Enum.reverse(rest, [{key, val}])
  end

  defp put([{khead, _} = head | tail] = list, key, val, rest, cmp) do
    cond do
      cmp.(key, khead) -> #key <= khead
        Enum.reverse(rest, [{key, val} | list])
      true ->
        put(tail, key, val, [head | rest], cmp)
    end
  end

  def put_unique({queue, cmp}, key, val), do: {put_unique(queue, key, val, [], queue, cmp), cmp}

  defp put_unique([], key, val, rest, _original, _cmp) do
    Enum.reverse(rest, [{key, val}])
  end

  defp put_unique([{khead, vhead} = head | tail] = list, key, val, rest, original, cmp) do
    cond do
      cmp.(key, khead) && key != khead -> # key < khead
        # key is no longer equal to the next item, and we have not encountered an equal value, so place the item here.
        Enum.reverse(rest, [{key, val} | list])
      key == khead && val == vhead ->
        original
      true ->
        put_unique(tail, key, val, [head | rest], original, cmp)
    end
  end

  def empty?({[], _cmp}), do: true

  def empty?({_, _cmp}), do: false

  def size({queue, _cmp}), do: Enum.count(queue)

  def peek({[], _cmp}), do: nil

  def peek({[head | _], _cmp}), do: head

  def take({[], cmp} = q), do: {nil, q}

  def take({[head | list], cmp}), do: {head, {list, cmp}}

  def take_before({list, cmp}, limit) do
    {res, new_list} =
      list
      |> Enum.split_while(fn {key, el} -> le(cmp, key, limit) end)

    {res, {new_list, cmp}}
  end

  def take_all({list, cmp}) do {list, new(cmp)} end

  def trim({list, cmp}, num) do
    {Enum.take(list, num), cmp}
  end

  def delete({queue, cmp}, fun) do
    new_list =
      queue
      |> Enum.reject(fn {key, val} -> fun.(key, val) end)

    {new_list, cmp}
  end

  defp le(cmp, el1, el2), do: cmp.(el1, el2) && el1 != el2
end
