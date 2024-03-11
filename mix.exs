defmodule EctoFoundationdb.MixProject do
  use Mix.Project

  def project do
    [
      app: :ecto_foundationdb,
      version: "0.1.0",
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      elixirc_paths: elixirc_paths(Mix.env()),
      aliases: aliases(),
      included_applications: [:ex_fdbmonitor],
      dialyzer: [
        ignore_warnings: ".dialyzer_ignore.exs"
      ]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      # {:dep_from_hexpm, "~> 0.3.0"},
      {:erlfdb, git: "https://github.com/foundationdb-beam/erlfdb.git", branch: "main"},
      {:ecto, "~> 3.10"},
      {:ecto_sql, "~> 3.11"},
      {:jason, "~> 1.4"},
      {:credo, "~> 1.6", only: [:dev, :test, :docs]},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},

      # Clustered tests
      {:local_cluster, "~> 1.2", only: [:test]},
      {:ex_fdbmonitor,
       git: "https://github.com/foundationdb-beam/ex_fdbmonitor.git",
       branch: "main",
       runtime: false,
       only: [:test]},

      # Benchmarks
      {:benchee, "~> 1.0", only: :dev}
    ]
  end

  defp aliases do
    [
      test: "test --no-start",
      lint: [
        "format --check-formatted",
        "deps.unlock --check-unused",
        "credo --all --strict",
        "dialyzer --format short"
      ]
    ]
  end
end
