defmodule Dataflow.Window.PaneInfo do
  @type pane_timing :: :early | :on_time | :late | :unknown
  @type t :: %__MODULE__{
    first?: boolean,
    last?: boolean,
    timing: pane_timing,
    index: non_neg_integer,
    on_time_index: non_neg_integer
  }

  defstruct \
  first?: false,
  last?: false,
  timing: :unknown,
  index: 0,
  non_speculative_index: 0

  @doc """
  A `PaneInfo` to use for elements on and before initial window assignment.

  This is used for elements, including ones read from sources, before they have passed through a `GroupByKey` and become
  associated with a particular trigger firing.
  """
  defmacro no_firing do: quote bind_quoted: [struct: __MODULE__], do: %struct{first?: true, last?: true, timing: :unknown}

  @doc """
  A `PaneInfo` to use when there will be exactly one firing and it is on time.
  """
  defmacro on_time_and_only_firing do: quote bind_quoted: [struct: __MODULE__], do: %struct{first?: true, last?: true,
  timing: :on_time}
end
