defmodule Dataflow.DirectRunner.TransformEvaluator.WriteFile do
  use Dataflow.DirectRunner.TransformEvaluator

  alias Dataflow.Transforms.IO.WriteFile

  def init(%WriteFile{filename: filename}, _input, _tm) do
    {:ok, file} = File.open(filename, [:utf8, :write])
    {:ok, file}
  end

  def transform_elements(elements, file) do
    for {el, _, _, _} <- elements, do: IO.write(file, [el, "\n"])
    {[], file}
  end

  def finish(file) do
    File.close file
  end
end
