defmodule Dataflow.Utils.PipelineVisualiser do
  @moduledoc """
  Contains functionality to visualise a pipeline's state, for debugging only. Currently outputs DOT code.
  """
  alias Dataflow.Pipeline
  alias Dataflow.Pipeline.State
  alias Dataflow.Pipeline.AppliedTransform
  alias Dataflow.PValue
  import Dataflow.Utils

  def visualise(pipeline) do
    pipeline
    |> Pipeline._get_state
    |> do_visualise
  end

  def do_visualise(%State{transforms: transforms, values: values}) do
    # Map through the transforms, adding them as nodes
    {nodes, edges} =
      transforms
      |> Map.values
      |> Enum.reduce({[], []}, node_reducer(values))

    # Now output the graph
    make_dot_graph(nodes, edges)
    |> IO.puts
  end

  defp node_reducer(values) do
    fn %AppliedTransform{id: id, input: input_id, parent: parent} = at,
       {nodes, edges} ->

      # Get the data edges
      data_edges = get_data_edges(id, get_value(values, input_id))

      # Get the parent edges
      parent_edges = get_parent_edges(id, parent)

      {[{id, make_transform_label(at)} | nodes], data_edges ++ parent_edges ++ edges}
    end
  end

  defp get_value(values, value_id), do: Map.fetch! values, value_id

  defp get_data_edges(_consumer_id, %PValue{type: :dummy}) do
    []
  end

  defp get_data_edges(consumer_id, %PValue{producer: producer_id, label: label}) do
    [{:data, producer_id, consumer_id, label}]
  end

  defp get_parent_edges(_child_id, 0) do
    []
  end

  defp get_parent_edges(child_id, parent_id) do
    [{:parent, child_id, parent_id}]
  end



  defp make_dot_graph(nodes, edges) do
    # IO list
    [ "digraph {\n rankdir=LR; \n",
      Enum.map(nodes, &output_node/1),
      Enum.map(edges, &output_edge/1),
      "}"
    ]
  end

  defp output_node({id, label}) do
    "#{id}[label=\"#{label}\"];\n"
  end

  defp output_edge({:data, from, to, label}) do
    "#{from} -> #{to}[label=\"#{label}\"];\n"
  end

  defp output_edge({:parent, from, to}) do
    "#{from} -> #{to}[constraint=\"false\", style=\"dashed\"];\n"
  end

end
