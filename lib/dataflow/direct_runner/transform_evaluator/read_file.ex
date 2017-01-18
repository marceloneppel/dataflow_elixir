defmodule Dataflow.DirectRunner.TransformEvaluator.ReadFile do
  use Dataflow.DirectRunner.TransformEvaluator

  alias Dataflow.Transforms.IO.ReadFile

  def init(%ReadFile{filename: filename}) do
    File.open(filename, [:utf8, :read])
  end

  def produce_element(file) do
    {IO.read(file, :line), file}
  end

  def produce_elements(number, file) do
    elements = for _ <- 1..number, do: IO.read(file, :line)
    {elements, file}
  end

  def finish(file) do
    File.close file
  end
end
