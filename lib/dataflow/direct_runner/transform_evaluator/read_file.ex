defmodule Dataflow.DirectRunner.TransformEvaluator.ReadFile do
  use Dataflow.DirectRunner.TransformEvaluator

  alias Dataflow.Transforms.IO.ReadFile

  alias Dataflow.Utils.Time
  require Time

  alias Dataflow.DirectRunner.TimingManager, as: TM

  def init(%ReadFile{filename: filename}, input, timing_manager) do
    unless (Dataflow.PValue.dummy? input), do: raise "Input to ReadFile must be a dummy."
    {:ok, {File.open(filename, [:utf8, :read]), timing_manager}}
  end

  def produce_elements(number, {file, tm} = state) do
    {status, elements} = do_produce_elements(number, file)
    if status == :finished do
      TM.advance_input_watermark tm, Time.max_timestamp
    end
    {elements, state}
  end

  defp do_produce_elements(number, file, elements \\ [])

  defp do_produce_elements(0, _file, elements) do
    {:active, Enum.reverse(elements)}
  end

  defp do_produce_elements(number, file, elements) do
    case IO.read(file, :line) do
      {:error, reason} -> raise "An error occurred reading file: #{inspect reason}"
      :eof -> {:finished, Enum.reverse(elements)}
      data -> do_produce_elements(number - 1, file, [{data, Time.min_timestamp, [:global], []} | elements])
    end
  end

  def finish(file) do
    File.close file
  end
end
