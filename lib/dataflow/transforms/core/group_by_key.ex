defmodule Dataflow.Transforms.Core.GroupByKey do
  use Dataflow.PTransform, make_fun: [group_by_key: 0]

  defstruct []

  alias Dataflow.Utils.WindowedValue
  alias Dataflow.Transforms.Fns.DoFn

  def group_by_key, do: %__MODULE__{}

  defimpl PTransform.Callable do

    defp reify_windows_do_fn do
      fn ({key, value}, timestamp, windows, _label, _state) ->
        [{key, %WindowedValue{value: value, timestamp: timestamp, windows: windows}}]
      end
      |> DoFn.from_function
    end

    defp group_also_by_window_do_fn(_windowing) do
      %DoFn{
        start_bundle:
          fn x ->
            raise "TODO!"
          end,
        process: fn x -> raise "more processing magic here to tackle later" end
      }
    end

    def expand(_, input) do
      # This code path used in the local runner, cloud runners can take this as an atomic operation and do some magic.
      # Maybe we'll be able to do that at some point too?

      use Dataflow.Transforms.Core.ParDo
      use Dataflow.Transforms.Core.GroupByKeyOnly

      input
      ~> "reify_windows" -- par_do(reify_windows_do_fn())
      ~> "group_by_key" -- group_by_key_only()
      ~> "group_by_window" -- par_do(group_also_by_window_do_fn(:windowing_here)) # windowing? get this from input?
    end

  end

end
