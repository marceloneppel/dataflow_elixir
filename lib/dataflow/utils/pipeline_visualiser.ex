defmodule Dataflow.Utils.PipelineVisualiser do
  @moduledoc """
  Contains functionality to visualise a pipeline's state, for debugging only. Currently outputs DOT code.
  """
  alias Dataflow.Pipeline
  alias Dataflow.Pipeline.State
  alias Dataflow.Pipeline.AppliedTransform
  alias Dataflow.PValue

  def visualise(pipeline) do
    pipeline
    |> Pipeline._get_state
    |> do_visualise
  end

  def do_visualise(%State{transforms: transforms}) do
    # Map through the transforms, adding them as nodes
    {nodes, edges} =
      transforms
      |> Map.values
      |> Enum.reduce({[], []}, &node_reducer/2)

    # Now output the graph
    make_dot_graph(nodes, edges)
    |> IO.puts
  end

  defp node_reducer(
    %AppliedTransform{id: id, input: input, parent: parent} = at,
    {nodes, edges}
  ) do

    # Get the data edges
    data_edges = get_data_edges(id, input)

    # Get the parent edges
    parent_edges = get_parent_edges(id, parent)

    {[{id, make_label(at)} | nodes], data_edges ++ parent_edges ++ edges}
  end

  defp get_data_edges(_consumer_id, %PValue{type: :dummy}) do
    []
  end

  defp get_data_edges(consumer_id, %PValue{producer: producer_id}) do
    [{:data, producer_id, consumer_id}]
  end

  defp get_parent_edges(_child_id, 0) do
    []
  end

  defp get_parent_edges(child_id, parent_id) do
    [{:parent, child_id, parent_id}]
  end

  defp make_label(%AppliedTransform{label: nil, transform: transform}) do
    get_label_from_transform(transform)
  end

  defp make_label(%AppliedTransform{label: "", transform: transform}) do
    get_label_from_transform(transform)
  end

  defp make_label(%AppliedTransform{label: label, transform: transform}) do
    "#{label}\n{#{get_label_from_transform(transform)}}"
  end

  defp get_label_from_transform(%{__struct__: module}) do
    module
    |> Atom.to_string
    |> String.replace_leading("Elixir.Dataflow.Transforms.", "")
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

  defp output_edge({:data, from, to}) do
    "#{from} -> #{to};\n"
  end

  defp output_edge({:parent, from, to}) do
    "#{from} -> #{to}[constraint=\"false\", style=\"dashed\"];\n"
  end

end
