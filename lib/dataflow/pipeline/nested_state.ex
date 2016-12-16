defmodule Dataflow.Pipeline.NestedState do
    alias Dataflow.PValue

    @typep state :: %__MODULE__{
      stack:          [Dataflow.Pipeline.id],
      new_values:     [PValue.t],
      new_transforms: [Dataflow.Pipeline.AppliedTransform.t],
      fresh_id:       (() -> Dataflow.Pipeline.id)
    }

    @opaque t :: pid

    defstruct stack: [], new_values: [], new_transforms: [], fresh_id: nil

    def start_link(fresh_id) do
      Agent.start_link(fn -> %__MODULE__{fresh_id: fresh_id} end)
    end

    def push_context(pid, cid) do
      Agent.update(pid, fn %__MODULE__{stack: stack} = state -> %{state | stack: [cid | stack] } end)
    end

    def pop_context(pid) do
      Agent.get_and_update(pid, fn %__MODULE__{stack: [cid | ss]} = state -> {cid, %{state | stack: ss}} end)
    end

    def peek_context(pid) do
      Agent.get(pid, fn %__MODULE__{stack: [cid | _]} -> cid end)
    end

    def flush(pid) do
      out = Agent.get(pid, fn %__MODULE__{new_values: values, new_transforms: transforms} -> {values, transforms} end)
      Agent.stop(pid)
      out
    end

    def fresh_id(pid) do
      Agent.get(pid, fn %__MODULE__{fresh_id: fresh_id} -> fresh_id.() end)
    end

    def add_value(pid, value) do
      Agent.update(pid, fn %__MODULE__{new_values: vs} = state -> %{state | new_values: [value | vs]} end)
    end

    def add_transform(pid, transform) do
      Agent.update(pid, fn %__MODULE__{new_values: vs} = state -> %{state | new_transforms: [transform | vs]} end)
    end
  end
