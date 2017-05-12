defmodule Dataflow do
  defmacro __using__(_opts) do
    quote do
      alias Dataflow.Pipeline

      require Dataflow
      import Dataflow, only: [~>: 2, deftransform: 2, deftransform: 3]
    end
  end

  defmacro pvalue ~> transform_with_label do
    case transform_with_label do
      {:<>, _, [opts, transform]} ->
        quote bind_quoted: [pvalue: pvalue, transform: transform, opts: opts] do
          Dataflow.__apply_transform__(pvalue, transform, opts)
        end
      {:--, _, [label, transform]} ->
        quote bind_quoted: [pvalue: pvalue, transform: transform, label: label] do
          Dataflow.__apply_transform__(pvalue, transform, label: label)
        end
      transform ->
        quote bind_quoted: [pvalue: pvalue, transform: transform] do
          Dataflow.__apply_transform__(pvalue, transform, [])
        end
    end
  end

  def __apply_transform__(value, transform, opts) do
    do_apply_transform(value, transform, opts)
  end

  defp do_apply_transform(%Dataflow.PValue{pipeline: p} = value, transform, opts) do
    Dataflow.Pipeline.apply_transform p, value, transform, opts
  end

  defp do_apply_transform(%Dataflow.Pipeline{} = p, transform, opts) do
    Dataflow.Pipeline.apply_root_transform p, transform, opts
  end

  defp do_apply_transform(%Dataflow.Pipeline.NestedInput{} = value, transform, opts) do
    Dataflow.Pipeline.apply_nested_transform value, transform, opts
  end




  defmacro deftransform(fun_name, opts \\ [], do: block) do
    do_deftransform(fun_name, opts, block, true)
  end

  defmacro deftransformp(fun_name, opts \\ [], do: block) do
    do_deftransform(fun_name, opts, block, false)
  end

  defp do_deftransform(fun_name, opts, block, public) do
    # set up the module first
    mod_name = opts[:as] ||
      fun_name
      |> Macro.to_string
      |> Macro.camelize
      |> List.wrap
      |> Module.concat

    expanded_block = expand_defexpands(block)

    mod_ast =
      quote do
        defmodule unquote(mod_name) do
          defstruct args: []

          unquote(expanded_block)

          defimpl Dataflow.PTransform.Callable do
            def expand(input, %{args: args}) do
              mod = unquote(mod_name)
              apply(mod, :__expand__, [input | args])
            end
          end
        end
      end

    # now turn all the defexpand clauses into functions which will construct the appropriate struct

    # get a list of all unique arities we have to support
    arg_nums = get_argnums(block)

    fun_asts =
      for arity <- arg_nums do
        arg_names =
          if arity == 0 do
            []
          else
            for num <- 0..(arity-1), arg_str = "arg#{num}", do: String.to_atom(arg_str)
          end

        arg_asts = Enum.map(arg_names, &Macro.var(&1, Elixir))

        func =
          quote do
            unquote(fun_name)(unquote_splicing(arg_asts)) do
              binds = binding()
              saved_args =
                unquote(arg_names)
                |> Enum.map(&Keyword.fetch!(binds, &1))

              %unquote(mod_name){args: saved_args}
            end
          end

        if public do
          quote do
            def unquote(func)
          end
        else
          quote do
            defp unquote(func)
          end
        end
      end

    [mod_ast | fun_asts]
  end

  defp expand_defexpands({:__block__, ctx, parts}) do
    parts = Enum.map(parts, &expand_defexpand/1)
    {:__block__, ctx, parts}
  end

  defp expand_defexpand({:defexpand, _, args}) do
    # blocks list is the last argument
    {blocks, arguments} = List.pop_at(args, -1)

    quote do
      def(__expand__(unquote_splicing(arguments)), unquote(blocks))
    end
  end

  defp expand_defexpand({ddef, _, [{name, _, _}]}) when ddef in [:def, :defp, :defmacro, :defmacrop] and name == :__expand__ do
    raise "You cannot define functions or macros named `__expand__` in a `deftransform` block. Please use a different name."
  end

  defp expand_defexpand(any), do: any

  defp get_argnums({:__block__, ctx, parts}) do
    argnums =
      parts
      |> Enum.flat_map(&get_defexpand_argnum/1)
      |> Enum.uniq
  end

  defp get_defexpand_argnum({:defexpand, _, args}) do
    {_blocks, arguments} = List.pop_at(args, -1)

    # remove first argument which is input
    arguments = tl arguments

    get_all_arities(arguments)
  end

  defp get_defexpand_argnum(any), do: []

  defp get_all_arities(args, current_arities \\ [0])

  defp get_all_arities([{:\\, _, _} | args], current_arities) do
    # optional argument, so arity can include it or not

    get_all_arities(args, current_arities)
    ++
    get_all_arities(args, increment_arities(current_arities))
  end

  defp get_all_arities([_arg | args], current_arities) do
    # required argument, so arity must include it
    get_all_arities(args, increment_arities(current_arities))
  end

  defp get_all_arities([], current_arities) do
    current_arities
  end

  defp increment_arities(arities) do
    Enum.map(arities, &(&1+1))
  end
end
