defmodule Dataflow.Mixfile do
  use Mix.Project

  def project do
    [app: :dataflow_elixir,
     version: "0.1.0",
     elixir: "~> 1.4",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps()]
  end

  # Configuration for the OTP application
  #
  # Type "mix help compile.app" for more information
  def application do
    [applications: [:logger]]
  end

  # Dependencies can be Hex packages:
  #
  #   {:mydep, "~> 0.3.0"}
  #
  # Or git/path repositories:
  #
  #   {:mydep, git: "https://github.com/elixir-lang/mydep.git", tag: "0.1.0"}
  #
  # Type "mix help deps" for more examples and options
  defp deps do
    [
      {:ex_doc, "~> 0.14", only: :dev},
      {:credo, "~> 0.5", only: [:dev, :test]},
      {:gen_stage, "~> 0.11"},
      {:apex, "~>0.7.0"},
      {:priority_queue, "~>1.0"},
      {:dialyxir, "~> 0.5", only: [:dev], runtime: false},
      {:twittex, "~> 0.2"}
    ]
  end
end
