defmodule DataflowTest do
  use ExUnit.Case
  doctest Dataflow
  use Dataflow, transforms: :all

  test "a pipeline can be created" do
    _ = Pipeline.new
  end

  test "a source can be added to the pipeline yielding a valid PValue" do
    p = Pipeline.new

    v = Pipeline.add_source p, :magic
    assert pvalue? v
    assert valid_pvalue? v
  end

  test "multiple transforms can be applied to the pipeline, yielding a valid PValue" do
    p = Pipeline.new

    v = p
    ~> create_dummy
    ~> dummy_root_xform
    ~> dummy_xform

    assert valid_pvalue? v
  end

  test "transforms can be named" do
    p = Pipeline.new

    v = p
    ~> "Create dummy data" -- create_dummy
    ~> "First xform" -- dummy_root_xform
    ~> "Second xform" -- dummy_xform

    assert valid_pvalue? v
  end

  test "a PValue becomes invalid once its context Pipeline is destroyed" do
    p = Pipeline.new

    v = Pipeline.add_source p, :magic
    assert pvalue? v
    assert valid_pvalue? v

    Pipeline.destroy p
    assert pvalue? v
    assert not valid_pvalue? v
  end

end
