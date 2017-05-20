defmodule TestTwitter do
  use GenServer

  def start_link(time) do
    GenServer.start_link(__MODULE__, time, name: __MODULE__)
  end

  def init(time) do
    Process.send_after(self(), :output, time * 1000)
    {:ok, {0, time}}
  end

  def handle_cast(:tweet, {num, time}) do
    {:noreply, {num + 1, time}}
  end

  def handle_info(:output, {num, time}) do
    IO.inspect num
    Process.send_after(self(), :output, time * 1000)
    {:noreply, {num, time}}
  end
end

TestTwitter.start_link(10)
stream = ExTwitter.stream_filter(track: "tech,technology,Apple,Google,Twitter,Facebook,Microsoft,iPhone,Mac,Android,computers,CompSci,science", language: "en");

stream
|> Stream.each(fn _ -> GenServer.cast(TestTwitter, :tweet) end)
|> Stream.run
