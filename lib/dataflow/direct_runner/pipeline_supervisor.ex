defmodule Dataflow.DirectRunner.PipelineSupervisor do
  use Supervisor
  alias Dataflow.DirectRunner.{TransformExecutor, ValueStore}

  def start_link(leaf_transforms, values) do
    Supervisor.start_link(__MODULE__, {leaf_transforms, values}, name: __MODULE__)
  end

  def init({leaf_transforms, values}) do
    # something to store values in
    store = worker(ValueStore, [values])

    # the registry will need to be started first, so we push it to the front of the list at the end
    transform_registry = supervisor(Registry, [:unique, Dataflow.DirectRunner.TransformRegistry])

    workers =
      leaf_transforms
      |> Enum.sort
      |> Enum.map(fn {tid, transform} -> worker(TransformExecutor, [transform], id: tid, restart: :transient) end)

    supervise([store, transform_registry | workers], strategy: :one_for_one) #TODO: consider the correct strategy here
  end
end
