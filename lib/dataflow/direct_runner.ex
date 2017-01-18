defmodule Dataflow.DirectRunner do
  @moduledoc """
  A direct runner which will execute a pipeline locally across available cores.

  Currently, relies on some named processes, so only one instance can exist at a time. This will change.
  """

  require Logger

  use Dataflow.Runner

  alias Dataflow.{Pipeline, Pipeline.State, Pipeline.AppliedTransform}

  def run(pipeline) do
    state = Pipeline._get_state(pipeline)

    # TODO: Do some coalescing of tasks?

    # Get leaf transforms & consumer map and extract some variables

    {leaf_transforms, consumers} = calculate_transforms_consumers(state)
    %{transforms: transforms, values: values} = state

    # TODO Verify that all values are actually being consumed? (but what about sinks)


  end

  defp calculate_transforms_consumers(%State{transforms: transforms, values: values}) do
    transforms
    |> Map.values
    |> Enum.reduce({%{}, %{}}, calculate_transforms_consumers_reducer(values))
  end

  defp calculate_transforms_consumers_reducer(values) do
    fn
      %AppliedTransform{id: transform_id, input: input_id, parts: []} = at,
      {leaf_xforms, consumers} ->
      # No parts, hence a leaf transform.
      {Map.put(leaf_xforms, transform_id, at), add_consumer_to_list(consumers, input_id, transform_id)}
    end
  end

  defp add_consumer_to_list(consumers, value_id, consumer_id) do
    Map.update(consumers, value_id, [consumer_id], fn cs_list -> [consumer_id | cs_list] end)
  end
end
