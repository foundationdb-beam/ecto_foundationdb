defmodule EctoFoundationdb.MixProject do
  use Mix.Project

  def project do
    [
      app: :ecto_foundationdb,
      version: "0.1.0",
      description: "FoundationDB adapter for Ecto",
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      elixirc_paths: elixirc_paths(Mix.env()),
      aliases: aliases(),
      dialyzer: [
        ignore_warnings: ".dialyzer_ignore.exs"
      ],
      package: package(),

      # Docs
      name: "Ecto.Adapters.FoundationDB",
      docs: docs()
    ]
  end

  defp package() do
    [
      licenses: ["Apache-2.0"],
      links: %{
        "GitHub" => "https://github.com/foundationdb-beam/ecto_foundationdb"
      }
    ]
  end

  defp docs do
    [
      main: "Ecto.Adapters.FoundationDB",
      source_url: "https://github.com/foundationdb-beam/ecto_foundationdb",
      filter_modules:
        ~r/^Elixir.Ecto.Adapters.FoundationDB|EctoFoundationDB(.Database|.Exception.Unsupported|.Exception.IncorrectTenancy|.Index|.Indexer|.Layer|.Migrator|.Options|.QueryPlan|.Sandbox|.Tenant)?$/,
      extras: ["notebooks/guide.livemd"]
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
      {:erlfdb, "~> 0.1.0"},
      {:ecto, "~> 3.10"},
      {:jason, "~> 1.4"},
      {:credo, "~> 1.6", only: [:dev, :test, :docs]},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.16", only: :dev, runtime: false},
      {:benchee, "~> 1.0", only: :dev}
    ]
  end

  defp aliases do
    [
      lint: [
        "format --check-formatted",
        "deps.unlock --check-unused",
        "credo --all --strict",
        "dialyzer --format short"
      ]
    ]
  end
end
