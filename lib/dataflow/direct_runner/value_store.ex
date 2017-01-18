defmodule Dataflow.DirectRunner.ValueStore do
  # just a simple agent to store a map of PValues

  def start_link(values) do
    Agent.start_link(fn -> values end, name: __MODULE__) #TODO dynamic register
  end

  def get(value_id) do
    Agent.get(__MODULE__, &Map.get(&1, value_id))
  end

end
