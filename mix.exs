defmodule Convoy.MixProject do
  use Mix.Project

  def project do
    [
      app: :convoy,
      version: "0.1.0",
      elixir: "~> 1.8",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {Convoy.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:hackney, "~> 1.15"},
      {:ex_aws, "~> 2.1"},
      {:ex_aws_kinesis, "~> 2.0"},
      {:jason, "~> 1.2"},
      {:gen_stage, "~> 1.0"},
      {:mox, "~> 0.4", only: :test},
      {:telemetry, "~> 0.4.1"}
    ]
  end
end
