defmodule Dataflow.DirectRunner.TransformEvaluator.WriteFile do
  use Dataflow.DirectRunner.TransformEvaluator

  alias Dataflow.Transforms.IO.WriteFile

  def init(%WriteFile{filename: filename}, _input) do
    File.open(filename, [:utf8, :write])
  end

  def consume_element({element, _timestamp, _windows}, file) do
    IO.write(file, [element, "\n"])
    file
  end

  def consume_elements(elements, file) do
    for {el, _, _} <- elements, do: IO.write(file, [el, "\n"])
    file
  end

  def finish(file) do
    File.close file
  end
end
