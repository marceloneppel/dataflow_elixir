defmodule Dataflow.DirectRunner.TransformEvaluator.ReadStream do
  use Dataflow.DirectRunner.TransformEvaluator

  alias Dataflow.Transforms.IO.ReadStream

  alias Dataflow.Utils.Time
  require Time

  alias Dataflow.DirectRunner.TimingManager, as: TM
  alias Dataflow.DirectRunner.TransformExecutor, as: TX

  def init(%ReadStream{make_stream: mk_stream, chunk_size: csize, full_elements?: felems?}, input, timing_manager) do
    unless (Dataflow.PValue.dummy? input), do: raise "Input to ReadStream must be a dummy."
    {:ok, task_pid} = Task.start_link(streamer_task(mk_stream, csize, felems?))
    {:ok, {task_pid, timing_manager}}
  end

  def produce_elements(number, state) do
    # TODO actually evaluate the stream lazily
    {[], state}
  end

  def handle_async({:chunk, chunk}, state) do
    {chunk, state}
  end

  def handle_async(:done, {_task_pid, timing_manager}) do
    TM.advance_input_watermark(timing_manager, Time.max_timestamp)
  end

  def finish({task_pid, _timing_manager}) do
    Task.shutdown(task_pid)
  end

  defp streamer_task(make_stream, chunk_size, felems?) do
    process = self()
    fn ->
      stream = make_stream.()

      stream = if (felems?) do
        stream
      else
        stream
        |> Stream.map(fn el -> {el, Time.min_timestamp, [:global], []} end)
      end

      stream
      |> Stream.chunk(chunk_size, chunk_size, [])
      |> Stream.each(fn chunk -> TX.notify_evaluator process, {:chunk, chunk} end)
      |> Stream.run

      TX.notify_evaluator(process, :done)
    end
  end
end
