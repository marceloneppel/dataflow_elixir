defmodule Dataflow.DirectRunner do
  @moduledoc """
  A direct runner which will execute a pipeline locally across available cores.
  """

  use Dataflow.Runner

  alias Dataflow.{Pipeline}

  def run(pipeline) do
    state = Pipeline._get_state(pipeline)

    # TODO: Do some coalescing of tasks?

    # Get leaf transforms & consumer map



    # Spin up a supervisor task for each stage in the pipeline
    # Connect them together correctly
    # Start any sources?
    # ???
    # Profit
  end
end
