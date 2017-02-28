defmodule Dataflow.DirectRunner.TransformEvaluator.WriteFile do
  use Dataflow.DirectRunner.TransformEvaluator

  alias Dataflow.Transforms.IO.WriteFile

  def init(%WriteFile{filename: filename}, _input) do
   {:ok, :no_update, File.open(filename, [:utf8, :write])}
  end

  def consume_element({element, _timestamp, _windows}, file) do
    IO.write(file, [element, "\n"])
    {:no_update, file}
  end

  def consume_elements(elements, file) do
    for {el, _, _, _} <- elements, do: IO.write(file, [el, "\n"])
    {:no_update, file}
  end

  def finish(file) do
    File.close file
  end
end
