defprotocol Dataflow.PTransform.Protocol do
  @moduledoc """
  Defines a protocol for things which can be applied to a `Pipeline` or `PValue` (generallly PTransforms).
  """

  def apply(data, nested_input)
end

#It's preferred to explicitly implement this for performance reasons
defimpl Dataflow.PTransform.Protocol, for: Any do
  def apply(%{__struct__: module} = data, nested_input) do
    Kernel.apply(module, :apply, [data, nested_input])
  end

  def apply(_, _) do
    raise "Trying to apply an invalid value to a Dataflow Pipeline. Implement #{__MODULE__} if you wish to be able to do this."
  end
end
