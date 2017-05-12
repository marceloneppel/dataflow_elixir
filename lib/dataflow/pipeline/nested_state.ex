defmodule Dataflow.Pipeline.NestedState do
    alias Dataflow.PValue

    @typep state :: %__MODULE__{
      stack:          [{Dataflow.Pipeline.id, [Dataflow.Pipeline.id]}],
      new_values:     [PValue.t],
      new_transforms: [Dataflow.Pipeline.AppliedTransform.t],
      fresh_id:       (() -> Dataflow.Pipeline.id),
      pipeline:       Dataflow.Pipeline.t,
    }

    @opaque t :: pid

    defstruct stack: [], new_values: [], new_transforms: [], fresh_id: nil, pipeline: nil, extra_opts: []

    def start_link(pipeline, fresh_id) do
      Agent.start_link(fn -> %__MODULE__{fresh_id: fresh_id, pipeline: pipeline} end)
    end

    def push_context(pid, cid) do
      Agent.update(pid, fn %__MODULE__{stack: stack} = state -> %{state | stack: [{cid, []} | stack]} end)
    end

    def set_extra_opts(pid, opts) do
      Agent.update(pid, fn state -> put_in(state.extra_opts, opts) end)
    end

    def get_extra_opts(pid) do
      Agent.get(pid, fn state -> state.extra_opts end)
    end

    def pop_context(pid) do
      Agent.get_and_update(pid,
        fn %__MODULE__{stack: [top | stack]} = state ->
          {
            top,
            %{state | stack: stack}
          }
        end
      )
    end

    def peek_context(pid) do
      Agent.get(pid, fn %__MODULE__{stack: [{cid, _} | _]} -> cid end)
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
      Agent.update(pid, fn %__MODULE__{new_values: values} = state -> %{state | new_values: [value | values]} end)
    end

    def add_transform(pid, transform) do
      Agent.update(pid,
        fn %__MODULE__{new_transforms: transforms, stack: [{cid, parts} | stack]} = state ->
          %{state | new_transforms: [transform | transforms], stack: [ {cid, [transform.id | parts]} | stack]}
        end
      )
    end

    def pipeline(pid) do
      Agent.get(pid, fn %__MODULE__{pipeline: pipeline} -> pipeline end)
    end
  end
