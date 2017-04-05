defmodule Dataflow.Pipeline do
  @moduledoc """
  A pipeline structure that manages a DAG of PValues and their PTransforms.
  Conceptually the PValues are the DAG's nodes and the PTransforms computing
  the PValues are the edges.
  All the transforms applied to the pipeline must have distinct full labels.
  If same transform instance needs to be applied then a clone should be created
  with a new label.
  """

  use GenServer

  alias Dataflow.PValue
  alias Dataflow.PTransform
  alias Dataflow.Pipeline.{AppliedTransform, NestedState}

  require Logger

  @opaque id :: pos_integer

  defstruct pid: nil

  defmodule State do
    defstruct options: [], values: %{}, transforms: %{}, counter: nil

    def from_options(opts) do
      {:ok, counter_pid} = Agent.start_link(fn -> 1 end)
      %__MODULE__{options: opts, counter: counter_pid}
    end

    def add_value(%State{values: values} = state, %PValue{id: id} = value) do
      %{state | values: Map.put(values, id, value)}
    end

    def add_transform(%State{transforms: transforms} = state, %AppliedTransform{id: id} = transform) do
      %{state | transforms: Map.put(transforms, id, transform)}
    end

    def add_values(state, values) when is_list(values) do
      Enum.reduce values, state, &(add_value &2, &1)
    end

    def add_transforms(state, transforms) when is_list(transforms) do
      Enum.reduce transforms, state, &(add_transform &2, &1)
    end

    def get_runner(%State{options: opts}) do
      opts[:runner]
    end
  end

  defmodule NestedInput do
    @type t :: %__MODULE__{
      value: PValue.value,
      state: NestedState.t
    }

    defstruct [:value, :state]
  end


  @doc """
  Initialises a pipeline data structure that can be operated on. The pipeline is a
  running process, so to clone it or transfer it you need to extract the state manually.
  """
  def new(opts \\ []) do
    # TODO maybe raise own exception on failure
    {:ok, pid} = GenServer.start_link(__MODULE__, opts)
    %__MODULE__{pid: pid}
  end

  def destroy(pipeline) do
    if pipeline?(pipeline) do
      GenServer.stop pipeline.pid
      :ok
    else
      {:error, :invalid_pipeline}
    end
  end

  def run(%__MODULE__{pid: pid} = pipeline, opts \\ []) do
    runner = GenServer.call(pid, {:get_runner})
    runner.run pipeline, opts
  end

  def _get_state(%__MODULE__{pid: pid}) do
    GenServer.call(pid, {:_get_state})
  end

  def apply_transform(%__MODULE__{pid: pid} = pipeline, value, transform, opts \\ []) do
    GenServer.call(pid, {:apply_transform, pipeline, value, transform, opts}, :infinity) #TODO remove timout
  end

  def apply_root_transform(%__MODULE__{pid: pid} = pipeline, transform, opts \\ []) do
    #TODO: ensure transform supports being root
    # PTransform.root_transform? transform

    value = GenServer.call(pid, {:add_value, pipeline, %PValue{type: :dummy, producer: 0}})

    apply_transform(pipeline, value, transform, opts)
  end

  def pipeline?(%__MODULE__{pid: p}) when is_pid(p) do
    #TODO tag process using process dictionary?
    Process.alive? p
  end

  def pipeline?(_), do: false

  def pvalue?(%PValue{pipeline: p}) when not is_nil(p), do: true
  def pvalue?(_), do: false

  def valid_pvalue?(%PValue{pipeline: p, id: v}) do
    pipeline?(p)
    && has_value? p, v
  end

  def valid_pvalue?(_), do: false

  def has_value?(pipeline, value) do
    GenServer.call(pipeline, {:has_value?, value})
  end

  # Private utilities
  defp fresh_id(%State{counter: pid}) do
    Agent.get_and_update(pid, fn id -> {id, id + 1} end)
  end

  # GenServer callbacks
  def init(opts) do
    {:ok, State.from_options opts}
  end

  def handle_call({:has_value?, value}, _from, state) do
    {:reply, Map.has_key?(state.values, value), state}
  end

  def handle_call({:apply_transform, pipeline, value, transform, opts}, _from, state) do
    #TODO: verify value is correct etc

    #TODO: for now assume value is a single PValue and not a composite

    # Make a new transform

    #TODO: take account of labels and so forth
    #TODO: factor out non-critical section code for better concurrency

    # Set up the nested state tracker
    {:ok, nested_state} = NestedState.start_link(pipeline, fn -> fresh_id(state) end)
    nested_input = %NestedInput{value: value, state: nested_state}

    NestedState.push_context(nested_state, 0) # Set the root transform as the root of the tree
    %NestedInput{value: output} = do_apply_transform(nested_input, transform, opts)  #Protocol polymorphism
    {0, _parts} = NestedState.pop_context(nested_state)
    # Invariant check, _parts is top-level transforms so no need to keep track
    # TODO: add _parts to the root transform list here??

    {new_values, new_transforms} = NestedState.flush(nested_state)

    # Add new values and transforms to the state
    state = state
    |> State.add_values(new_values)
    |> State.add_transforms(new_transforms)

    {:reply, output, state}
  end

  def handle_call({:add_value, pipeline, value}, _from, state) do
    #TODO: ensure value is value
    id = fresh_id(state)

    new_value = %{value | id: id, pipeline: pipeline}

    {:reply, new_value, State.add_value(state, new_value)}
  end

  def handle_call({:_get_state}, _from, state) do
    {:reply, state, state}
  end

  def handle_call({:get_runner}, _from, state) do
    {:reply, State.get_runner(state), state}
  end

  def apply_nested_transform(nested_input, transform, opts \\ []) do
    do_apply_transform(nested_input, transform, opts)
  end

  defp do_apply_transform(nested_input, transform, opts \\ []) do
    state = nested_input.state

    # Get an ID for our new transform
    id = NestedState.fresh_id(state)

    # Grab the parent id
    parent = NestedState.peek_context(state)

    # Make ourselves the new context
    NestedState.push_context(state, id)

    %NestedInput{value: output} = PTransform.expand transform, nested_input # {id, transform}

    # Pop the context
    {^id, parts} = NestedState.pop_context(state)
    # On invariant error the id pinned match will fail

    NestedState.add_value(state, output) # should this be handled by fresh_pvalue?
    NestedState.add_transform(state,
      %AppliedTransform{
        id: id,
        parent: parent,
        parts: parts,
        transform: transform,
        input: nested_input.value.id, #inputs?
        output: output.id, #outputs?
        pipeline: NestedState.pipeline(state),
        label: Keyword.get(opts, :label, "")
      }
    )



    %NestedInput{state: state, value: output}
  end


end
