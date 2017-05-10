defmodule Dataflow.DirectRunner.StatsCollector do
  use GenServer
  alias NimbleCSV.RFC4180, as: CSV

  def start_link(log_path) do
    GenServer.start_link(__MODULE__, log_path, name: __MODULE__)
  end

  # API

  def log_output_watermark(id, watermark) do
    proc_time =
      System.os_time() # technically may not be monotonic
#      DateTime.utc_now()
#      |> DateTime.to_unix

    GenServer.cast(__MODULE__, {:log_owm, id, watermark, proc_time})
  end

  # callbacks

  def init(log_parent_path) do
    path = Path.join(log_parent_path, "#{timestamp()}")
    File.mkdir_p! path
    {:ok, %{path: path, files: %{}}}
  end

  def handle_cast({:log_owm, id, watermark, proc_time} = msg, state) do
    state = init_file_if_needed(id, state)
    file = Map.fetch!(state.files, id)

    data =
      [[proc_time, watermark]]
      |> CSV.dump_to_iodata()

    :ok = IO.binwrite(file, data)

    {:noreply, state}
  end

  def terminate(_, %{files: files}) do
    # not really needed, since it will be done automatically once this process dies anyway
    Enum.each files, fn {_id, file} -> File.close file end
  end

  defp init_file_if_needed(id, state) do
    case Map.has_key?(state.files, id) do
      true -> state
      false ->
        file_path = Path.join(state.path, "#{id}_owm.csv")
        file = File.open!(file_path, [:write, :raw])
        header = CSV.dump_to_iodata([~w(proc_time, watermark)])
        :ok = IO.binwrite(file, header)

        put_in(state.files[id], file)
    end
  end

  defp timestamp do
    {{y, m, d}, {hh, mm, ss}} = :calendar.universal_time()
    "#{y}_#{pad(m)}_#{pad(d)}__#{pad(hh)}_#{pad(mm)}_#{pad(ss)}"
  end

  defp pad(i) when i < 10, do: << ?0, ?0 + i >>
  defp pad(i), do: to_string(i)
end
