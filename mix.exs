defmodule EctoFoundationdb.MixProject do
  use Mix.Project

  def project do
    [
      app: :ecto_foundationdb,
      version: "0.1.0",
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      elixirc_paths: elixirc_paths(Mix.env())
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
      {:erlfdb,
       git: "https://github.com/JesseStimpson/couchdb-erlfdb.git", branch: "feature/tenants"},
      {:ecto, "~> 3.10"}
    ]
  end
end
