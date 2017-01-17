defmodule Dataflow.Runner do
    @moduledoc """
    A behaviour which is implemented by internal modules
    """

    @doc """
    Executes the given pipeline.
    """
    @callback run(Dataflow.Pipeline.t) :: {:ok, pid} | {:error, any}

    @doc """
    A hook for custom implementations of the `expand` function for some transforms.

    For example, a cloud runner may want to include its own sub-transforms for certain operations, or on the other hand
    treat something like a `GroupByKey` as a primitive.

    If the default expansion should apply, just return `:pass`. This is the default fall-through implementation
    when this module is `use`d.
    """
    @callback expand_transform(Dataflow.PTransform.Callable.t) :: :pass | Dataflow.PValue.t

    defmacro __using__(_opts) do
      quote do
        @behaviour unquote(__MODULE__)

        @before_compile unquote(__MODULE__)
      end
    end

    defmacro __before_compile__(_env) do
      def expand_transform(_), do: :pass
    end
end
