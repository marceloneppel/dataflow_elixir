defmodule Dataflow.Utils.WindowedValue do
  @moduledoc """
  Represents a value which has been windowed, that is which has an inherent timestamp and a set of windows assigned to
  it.
  """

  defstruct value: nil, timestamp: nil, windows: []

  @type t :: %__MODULE__{
    value: any,
    timestamp: Dataflow.Utils.Time.timestamp,
    windows: [Dataflow.Window.t]
  }
end
