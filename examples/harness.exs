defmodule PSCollector do
  use GenServer
  alias NimbleCSV.RFC4180, as: CSV

  def start_link() do
    GenServer.start_link(__MODULE__, {}, name: __MODULE__)
  end

  # API

  def log_measurement(measurement) do
    proc_time =
      System.os_time(:microseconds) # technically may not be monotonic
#      DateTime.utc_now()
#      |> DateTime.to_unix

    GenServer.cast(__MODULE__, {:measure, measurement, proc_time})
  end

  def change_prefix(prefix) do
    GenServer.call(__MODULE__, {:change_prefix, prefix})
  end

  def change_path(path) do
    GenServer.call(__MODULE__, {:change_path, path})
  end

  # callbacks

  def init({}) do
    {:ok, %{path: nil, file: nil, prefix: nil}}
  end

  def handle_call({:change_path, path}, _from, state) do
    unless state.file == nil do
      File.close(state.file)
    end
    {:reply, :ok, %{state | file: nil, prefix: nil, path: path}}
  end

  def handle_call({:change_prefix, prefix}, _from, state) do
    unless state.file == nil do
      File.close(state.file)
    end
    state = put_in(state.prefix, prefix)
    state = init_file(state)

    {:reply, :ok, state}
  end

  def handle_cast({:measure, measurement, proc_time}, state) do
    {cpu, mem} = process_measurement(measurement)

    data =
      [[proc_time, cpu, mem]]
      |> CSV.dump_to_iodata()

    :ok = IO.binwrite(state.file, data)

    {:noreply, state}
  end

  # assume list of lines
  defp process_measurement([measurement | _]) do
    [cpu, mem] = String.split(measurement)
    {cpu, mem}
  end

  defp init_file(state) do
    subdir = Path.join([state.path, state.prefix])
    File.mkdir_p!(subdir)

    filename = Path.join(subdir, "cpumem.csv")
    file = File.open!(filename, [:write, :raw])
    header = CSV.dump_to_iodata([~w(time cpu mem)])
    :ok = IO.binwrite(file, header)

    %{state | file: file}
  end
end

defmodule TestHarness do
  use GenServer
  require Logger

  @log_main_path "../../eval-logs"
  @ps_interval 500

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def init(opts) do
    PSCollector.start_link()
    state = %{
      schedule: make_schedule(opts[:tasks]),
      ps_path: System.find_executable("ps"),
      harness_name: opts[:name] || "harness",
    }
    Logger.info("Created schedule, have #{Enum.count(state.schedule)} tasks to do. Starting harness.")
    schedule_next_task()
    {:ok, state}
  end

  def handle_cast(:next_task, %{schedule: []} = state) do
    Logger.info("Finished processing all tasks. Exiting.")
    {:stop, :normal, state}
  end

  def handle_cast(:next_task, state) do
    [task | schedule] = state.schedule

    state =
      state
      |> put_in([:schedule], schedule)
      |> put_in([:current_task], task)
      |> put_in([:current_rep], -1)

    Logger.info("Now executing task #{pad(task.index)} (#{task.lang}:#{task.test}, x#{task.repeat}@#{task.runtime}s).")

    PSCollector.change_path(make_path(task, state))

    # now schedule execution of the next rep
    schedule_next_rep()
    {:noreply, state}
  end

  def handle_cast(:next_rep, state) do
    state = put_in(state.current_rep, state.current_rep + 1)
    if state.current_rep == state.current_task.repeat do
      Logger.info("Finished all reps, scheduling next task.")
      schedule_next_task()
      {:noreply, state}
    else
      execute_rep(state)
    end
  end

  defp execute_rep(state) do
    task = state.current_task
    rep_no = state.current_rep
    Logger.debug("Starting execution of rep ##{pad(rep_no)}...")

    prefix = make_prefix(task, rep_no)
    path = make_path(task, state)

    PSCollector.change_prefix(prefix)

    args =
      task.args
      |> Map.put(:log_prefix, prefix)
      |> Map.put(:log_path, path)

    # TODO If soft runtime, add timeout to args

    cmd = cmd(task.lang, task.test) ++ Enum.flat_map(args, &arg(task.lang, task.test, &1))

    Logger.debug(fn -> "Running `#{Enum.join cmd, " "}`" end)

    # Start the process
    opts =
      [monitor: true, stderr: :print, env: %{"MIX_ENV" => "prod"}]
      ++
      if task.runtime_method == :kill do
        [stdout: true] # capture stdout to know when pipeline starts
      else
        [] # Pipeline manages itself.
      end


    {:ok, manager_pid, os_pid} = Exexec.run(cmd, opts)
    timer = schedule_measurement_timer(os_pid)

    rundata = %{
      manager_pid: manager_pid,
      os_pid: os_pid,
      timer_ref: timer,
      log_path: path,
      killed?: false,
      timer_kill: nil
    }

    Logger.info("Started execution of rep ##{pad(rep_no)} with PID #{os_pid}.")

    state = put_in(state[:rundata], rundata)
    {:noreply, state}
  end

  def handle_info({:stdout, os_pid, "--STARTED--\n"}, state) do
    if state.rundata.os_pid != os_pid do
      Logger.warn "stdout received for unknown os process #{os_pid}."
      {:noreply, state}
    else
      state =
        if state.rundata.timer_kill != nil do
          Logger.warn "Kill timer already set but STARTED message seen."
          state
        else
          timer_kill = schedule_kill_timer(os_pid, state.current_task.runtime * 1000)
          Logger.info "Pipeline started and kill timer scheduled."
          put_in(state.rundata.timer_kill, timer_kill)
        end
      {:noreply, state}
    end
  end

  def handle_info({:stdout, os_pid, out}, state) do
    {:noreply, state}
  end

  def handle_info({:DOWN, os_pid, :process, _ex_pid, reason}, state) do
    # check if this is the current process we are monitoring?
    Logger.debug("Got :DOWN from an OS process.")
    if state.rundata.os_pid != os_pid do
      Logger.warn "DOWN status received for unknown os process #{os_pid}."
      {:noreply, state}
    else
      status =
        case reason do
          :normal -> 0
          {:exit_status, s} -> s
        end
      handle_DOWN(state.current_task.runtime_method, status, state)
    end
  end

  defp handle_DOWN(:kill, _status, state) do
    if not state.rundata.killed? do
      # we didn't kill this process, and so we should not be receiving a kill signal
      raise "Process unexpectedly died."
    end

    # Timers got cleaned up on kill timer
    # All done. Process is done, time to do the next rep.
    schedule_next_rep()
    {:noreply, state}
  end

  defp handle_DOWN(:wait, 0, state) do
    Logger.debug("Got successful termination.")
    # Successful termination.
    # cancel timer
    Process.cancel_timer(state.timer_ref)

    # All done. Process is done, time to do the next rep.
    schedule_next_rep()
    {:noreply, state}
  end

  defp handle_DOWN(:wait, _status, state) do
    raise "Process unexpectedly died with a non-0 error code."
  end

  def handle_info({:measure, os_pid}, state) do
    if state.rundata.os_pid != os_pid do
      Logger.warn "Measure request received for unknown os process #{os_pid}."
      {:noreply, state}
    else
      handle_measure(state)
    end
  end

  defp handle_measure(state) do
    os_pid = state.rundata.os_pid
    cmd = [state.ps_path, "-p#{os_pid}", "-opcpu=", "-orss="]
    {:ok, result} = Exexec.run(cmd, sync: true, stdout: true)
    lines = Keyword.fetch!(result, :stdout)
    send_measurement(lines, state)

    timer = schedule_measurement_timer(os_pid)
    state = put_in(state.rundata.timer_ref, timer)
    {:noreply, state}
  end

  def handle_info({:kill, os_pid}, state) do
    if state.rundata.os_pid != os_pid do
      Logger.warn "Kill request received for unknown os process #{os_pid}."
      {:noreply, state}
    else
      Logger.info("Stopping running process...")
      Process.cancel_timer(state.rundata.timer_ref)
      Exexec.stop(state.rundata.manager_pid) # try to kill gracefully.
      # we will receive a :DOWN message and then proceed.
      {:noreply, put_in(state.rundata.killed?, true)}
    end
end

  defp send_measurement(measurement, state) do
    PSCollector.log_measurement(measurement)
  end

  defp schedule_measurement_timer(os_pid) do
    Process.send_after(__MODULE__, {:measure, os_pid}, @ps_interval)
  end

  defp schedule_kill_timer(os_pid, delay) do
    Process.send_after(__MODULE__, {:kill, os_pid}, delay)
  end

  defp schedule_next_task() do
    GenServer.cast(__MODULE__, :next_task)
  end

  defp schedule_next_rep() do
    GenServer.cast(__MODULE__, :next_rep)
  end

  defp cmd(:elixir, :latency) do
    mix = System.find_executable("mix")
    [mix, "run", "examples/latency.exs"]
  end

  defp arg(:elixir, :latency, {:stream_delay, delay}) do
    ["--stream-delay", to_string(delay)]
  end

  defp arg(:elixir, :latency, {:num_transforms, num}) do
    ["--num-extra-transforms", to_string(num)]
  end

  defp arg(:elixir, :latency, {:log_prefix, prefix}) do
    ["--log-prefix", prefix]
  end

  defp arg(:elixir, :latency, {:log_path, path}) do
    ["--log-path", path]
  end

  defp runtime_method(:elixir), do: :kill
  defp runtime_method(:java), do: :wait

  defp make_prefix(task, rep) do
    "#{task.lang}_#{task.test}_#{pad(task.index)}_#{pad(rep)}"
  end

  defp log_subpath(state) do
    state.harness_name
  end

  defp make_path(task, state) do
    Path.join([Path.expand(@log_main_path), log_subpath(state), "#{pad(task.index)}_#{task.lang}_#{task.test}_#{task.id}"])
  end

  defp make_schedule(nil) do
    raise "Need to pass in tasks."
  end

  defp make_schedule(tasks) do
    tasks
    |> Enum.with_index()
    |> Enum.map(&make_task/1)
  end

  defp make_task({%{
    lang: lang,
  } = task, index}) do

    runtime_method = runtime_method(lang)

    task
    |> put_in([:runtime_method], runtime_method)
    |> put_in([:index], index)
  end

  defp pad(num) do
    num
    |> Integer.to_string()
    |> String.rjust(3, ?0)
  end
end

transformnumbers = [1, 3, 8, 22, 60, 167, 464, 1292, 3594, 10000]

tasks =
  transformnumbers
  |> Enum.map(fn num ->
    %{
      id: "nt#{num}",
      lang: :elixir,
      test: :latency,
      repeat: 1,
      runtime: 2,
      args: %{
        stream_delay: 10,
        num_transforms: num
      }
    }
   end)

{:ok, pid} = TestHarness.start_link(tasks: tasks, name: "exonly2-logspace-testonly")
ref = Process.monitor(pid)

receive do
  {:DOWN, ^ref, _, _, _} -> :ok
end




