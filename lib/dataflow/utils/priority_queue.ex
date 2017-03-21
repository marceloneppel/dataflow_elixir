defmodule Dataflow.Utils.PriorityQueue do
  @moduledoc """
  Extremely simple, inefficient priority queue using a naive list implementation.

  Should be replaced by something more efficient.
  """

  def new, do: []

  def put(queue, key, val), do: put(queue, key, val, [])

  def put([], key, val, rest) do
    Enum.reverse(rest, [{key, val}])
  end

  def put([{khead, _} | _] = list, key, val, rest) when key < khead do
    Enum.reverse(rest, [{key, val} | list])
  end

  def put([head | tail], key, val, rest) do
    put(tail, key, val, [head | rest])
  end

  def empty?([]), do: true

  def empty?(_), do: false

  def size(queue), do: Enum.count(queue)

  def peek([]), do: nil

  def peek([head | _]), do: head

  def take([]), do: nil

  def take([head | list]), do: {head, list}

  def take_before(list, limit) do
    list
    |> Enum.split_while(fn {key, el} -> key < limit end)
  end

  def take_all(list) do {list, new()} end

  def delete(queue, fun) do
    queue
    |> Enum.reject(fn {key, val} -> fun.(key, val) end)
  end
end
