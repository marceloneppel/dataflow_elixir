defmodule Dataflow.Trigger do
  defprotocol Triggerable do
    alias Dataflow.{Utils.Time, Window}

    # This is not necessarily user-extensible as specific TriggerDrivers must be defined in the runner.

    @spec continuation_trigger(t) :: t
    def continuation_trigger(trigger)


    @spec watermark_that_guarantees_firing(t, Window.t) :: Time.timestamp
    def watermark_that_guarantees_firing(trigger, window)


  end

  @type t :: Triggerable.t

  @doc """
  Return a trigger to use after a `GroupByKey` to preserve the intention of this trigger.
  Specifically, triggers that are time-based and intended to provide speculative results should continue to provide
  speculative results. Triggers that fire once (or multiple times) should continue firing once (or multiple times).
  """
  @spec continuation_trigger(t) :: t
  defdelegate continuation_trigger(trigger), to: Triggerable

  @doc """
  Returns an event-time bound by which this triggers must have fired at least once for a window had there been input data.

  For triggers that fo not fire based on the watermark advancing, returns `Dataflow.Utils.Time.max_timestamp/0`.

  This estimate may be used, for example, to determine that there are no elements in a side-input window, which causes
  the default value to be used instead.
  """
  @spec watermark_that_guarantees_firing(t, Window.t) :: Time.timestamp
  defdelegate watermark_that_guarantees_firing(trigger, window), to: Triggerable

  defmacro default, do: quote do: %Dataflow.Trigger.Default{}
end
