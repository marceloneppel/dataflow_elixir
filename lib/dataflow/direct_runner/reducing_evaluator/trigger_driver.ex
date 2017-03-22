defmodule Dataflow.DirectRunner.ReducingEvaluator.TriggerDriver do
  alias Dataflow.Utils.Time
  require Time

  alias Dataflow.{Trigger, Window}
  alias Dataflow.DirectRunner.TriggerDrivers

  @opaque state :: any
  @type t :: module

  @callback init(Trigger.t, Window.t, timing_manager :: pid) :: state

  @callback process_element(state, Time.timestamp) :: state

  @callback merge([state], state) :: state

  @callback should_fire?(state) :: boolean

  @callback fired(state) :: state

  @callback finished?(state) :: boolean

#  @callback get_sub_triggers()

  def module_for(%Trigger.Default{}), do: TriggerDrivers.Default

  def module_for(%{__struct__: module}), do: raise "No trigger driver available for trigger #{module}"
end
