defmodule Dataflow.Utils.TransformUtils do
  defmacro __using__(_opts) do
    quote do
      require(unquote(__MODULE__))
      import(unquote(__MODULE__))
    end
  end

  defmacro export_transforms(funs_to_modules) when is_map(funs_to_modules) do
    Enum.flat_map funs_to_modules, fn {fun, {module_name, ar}} ->
      arities = List.wrap(ar)
      Enum.map arities, fn arity -> make_delegate(fun, module_name, arity) end
    end
  end

  defmacro export_transforms(funs_to_modules) when is_list(funs_to_modules) do
    unless Keyword.keyword?(funs_to_modules), do: raise "Must pass arities"
    Enum.flat_map funs_to_modules, fn {module_name, ar} ->
      arities = List.wrap(ar)
      module_name = Macro.expand(module_name, __ENV__)
      fun =
        module_name
        |> to_string
        |> Macro.underscore
        |> String.to_atom
      Enum.map arities, fn arity -> make_delegate(fun, module_name, arity) end
    end
  end

  defp make_delegate(fun, module_name, arity) do
    args =
      case arity do
        0 -> []
        _ ->
          for num <- 0..(arity-1), do: {String.to_atom("arg#{num}"), [], Elixir}
      end
    quote do
      module = Module.concat(__MODULE__, unquote(module_name))
      defdelegate unquote(fun)(unquote_splicing(args)), to: module, as: :new
    end
  end
end
