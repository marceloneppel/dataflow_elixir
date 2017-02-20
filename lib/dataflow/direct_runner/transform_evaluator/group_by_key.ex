defmodule Dataflow.DirectRunner.TransformEvaluator.GroupByKey do
  use Dataflow.DirectRunner.TransformEvaluator, type: :reducing

  alias Dataflow.{Window, Utils.Time, PValue}
  require Time

  defmodule State do
    alias Dataflow.DirectRunner.TransformEvaluator

    @type key :: any
    @type value :: any
    @type trigger_result :: {:trigger, [Window.t]} | :no_trigger
    @type trigger_fun :: (Time.timestamp -> trigger_result)

    @type t :: %__MODULE__{
      elements: %{Window.t => %{key => [value]}},
      triggers: [trigger_fun], # a trigger function takes the current output timestamp
      input: PValue.t # the input to this function, used to read off the triggering/windowing semantics
    }

    defstruct elements: %{}, triggers: [], input: nil
  end

  def init(_, input) do
    #Todo read stuff from the input PValue to set up the correct windowing and triggers, etc.
    {:ok, %State{input: input}}
  end

  def transform_element(element, state) do
    {[], buffer_element(state, element)}
  end

  def transform_elements(elements, state) do
    {[], Enum.reduce(elements, state, fn el, st -> buffer_element(st, el) end)}
  end

  defp buffer_element(%State{elements: elements, triggers: triggers} = state, {{key, value}, timestamp, windows}) do
    case windows do
      [window] ->
        # Push the value into the correct window and key bin
        # TODO: record timestamps?

        # Check if the window is being tracked already, and if not, add a trigger for its end
        new_triggers =
          if elements[window] do
            triggers
          else
            add_triggers_for_window(triggers, window)
          end

        # Put the element in the correct list
        new_elements = Map.update elements, window, %{key => [value]}, fn els ->
          Map.update els, key, [value], fn values -> [value | values] end
        end

        %{state | elements: new_elements, triggers: new_triggers}

      windows ->
        # There is more than one window, so unpack and reduce the elements with the current function
        Enum.reduce windows, state, fn window, st -> buffer_element st, {{key, value}, timestamp, [window]} end
    end
  end

  defp add_triggers_for_window(triggers, :global) do
    # for now only implement a global end-of-window trigger
    trigger_timestamp = Time.max_timestamp
    trigger = fn
      ^trigger_timestamp -> {:trigger, [:global]} # trigger the global window once the output timestamp is max
      _ -> :no_trigger # else don't
    end

    [trigger | triggers]
  end

  def update_input_watermark(watermark, state) do
    # Fired whenever the input watermark has changed.
    # We need to fire any applicable triggers, and output an appropriate output watermark to propagate
    windows_to_trigger =
      state.triggers
      |> Enum.flat_map(fn trigger ->
          case trigger.(watermark) do
            :no_trigger -> []
            {:trigger, windows} -> windows
          end
        end)

    # get the triggered elements
    triggered_elements =
      windows_to_trigger
      |> Enum.flat_map(fn window ->
        state.elements[window]
        |> Enum.map(fn {key, values} ->
          {{key, values}, watermark, [window]} # TODO: use OutputTimeFn to determine the output timestamp?
         end)
      end)

    # remove the triggered elements from the window buffer
    # TODO control this?
    elements = Map.drop state.elements, windows_to_trigger

    #TODO also remove redundant triggers?

    {watermark, triggered_elements, %{state | elements: elements}}
  end

  def finish(_state) do
    :ok #all elements that should have been emitted already were?
  end
end
