defmodule Dataflow.DirectRunner.ReducingEvaluator.TriggerDriver do
  alias Dataflow.Utils.Time
  require Time

  alias Dataflow.{Trigger, Window}
  alias Dataflow.DirectRunner.TriggerDrivers

  @opaque state :: any
  @type t :: module

  @type timer_domain :: :event_time | :processing_time
  @type timer_cmd :: {:set, Time.timestamp, timer_domain} | {:clear, Time.timestamp, timer_domain}

  @callback init(Trigger.t, Window.t) :: state

  @callback process_element(state, Time.timestamp, event_time :: Time.timestamp) :: {[timer_cmd], state}

  @callback merge([state], state, event_time :: Time.timestamp) :: {[timer_cmd], state}

  @callback should_fire?(state, event_time :: Time.timestamp) :: boolean

  @callback fired(state, event_time :: Time.timestamp) :: state

  @callback finished?(state) :: boolean

#  @callback get_sub_triggers()

  def module_for(%Trigger.Default{}), do: TriggerDrivers.Default

  def module_for(%{__struct__: module}), do: raise "No trigger driver available for trigger #{module}"
end
