defmodule Dataflow.DirectRunner do
  @moduledoc """
  A direct runner which will execute a pipeline locally across available cores.

  Currently, relies on some named processes, so only one instance can exist at a time. This will change.
  """

  require Logger

  use Dataflow.Runner

  alias Dataflow.{Pipeline, Pipeline.State, Pipeline.AppliedTransform}
  alias Dataflow.DirectRunner.PipelineSupervisor

  def run(pipeline, opts \\ []) do
    state = Pipeline._get_state(pipeline)

    # TODO: Do some coalescing of tasks?

    # Get leaf transforms & consumer map and extract some variables

    {leaf_transforms, consumers} = calculate_transforms_consumers(state)
    %{transforms: _transforms, values: values} = state

#    Logger.debug "VALUES:\n\n#{Apex.Format.format values}"
#    Logger.debug "TRANSFORMS:\n\n#{Apex.Format.format transforms}"
#    Logger.debug "LEAF TRANSFORMS:\n\n#{Apex.Format.format leaf_transforms |> Map.keys}"

    # check for any values which are not being consumed, and modify them to be dummies
    non_consumed_values =
      consumers
      |> Enum.filter_map(fn {value, consumers} -> consumers == [] end, fn {value, consumers} -> value end)

    Logger.debug("Non consumed values: #{inspect non_consumed_values}")

    values =
      non_consumed_values
      |> Enum.reduce(values,
        fn dummy_id, values ->
          Map.update!(values, dummy_id, fn val ->
            %{val | type: :dummy}
          end)
        end)

    {:ok, pid} = PipelineSupervisor.start_link(leaf_transforms, values)

    if opts[:sync] do
      ref = Process.monitor pid
      receive do
        {:DOWN, ^ref, _, _, _} -> Logger.info "Pipeline exited"
      end
    end

  end

  defp calculate_transforms_consumers(%State{transforms: transforms, values: values}) do
    consumers = for {value_id, _} <- values, into: %{}, do: {value_id, []}

    transforms
    |> Map.values
    |> Enum.reduce({%{}, consumers}, calculate_transforms_consumers_reducer(values))
  end

  defp calculate_transforms_consumers_reducer(values) do
    fn
      %AppliedTransform{id: transform_id, input: input_id, parts: []} = at,
      {leaf_xforms, consumers} ->
      # No parts, hence a leaf transform.
      {Map.put(leaf_xforms, transform_id, at), add_consumer_to_list(consumers, input_id, transform_id, values)}

      _, acc -> acc # composite transform, so ignore
    end
  end

  defp add_consumer_to_list(consumers, value_id, consumer_id, values) do
    consumers = Map.update!(consumers, value_id, fn cs_list -> [consumer_id | cs_list] end)

    # Check for proxy values
    value = values[value_id]
    case value.type do
      :proxy ->
        {:proxy, from_id} = value.producer
        add_consumer_to_list(consumers, from_id, consumer_id, values)
      _ -> consumers
    end
  end
end
