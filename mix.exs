defmodule EctoFoundationdb.MixProject do
  use Mix.Project

  def project do
    [
      app: :ecto_foundationdb,
      version: "0.5.0",
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
        ~r/^Elixir.Ecto.Adapters.FoundationDB|EctoFoundationDB(.CLI|.Database|.Exception.Unsupported|.Exception.IncorrectTenancy|.Future|.Index|.Indexer|.Layer|.Migrator|.Options|.QueryPlan|.Sandbox|.Tenant|.Tenant.DirectoryTenant|.Tenant.ManagedTenant)?$/,
      extras: [
        "CHANGELOG.md",
        "docs/getting_started/introduction.livemd",
        "docs/getting_started/watches.livemd",
        "docs/developer_guides/testing.md",
        "docs/developer_guides/operators_manual.md",
        "docs/design/metadata.md"
      ],
      groups_for_extras: [
        "Getting Started": ~r/getting_started/,
        "Developer Guides": ~r/developer_guides/,
        Design: ~r/design/
      ],
      before_closing_head_tag: &docs_before_closing_head_tag/1,
      before_closing_body_tag: &docs_before_closing_body_tag/1
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: extra_applications(Mix.env())
    ]
  end

  defp extra_applications(:test), do: [:logger, :runtime_tools]
  defp extra_applications(_), do: [:logger]

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:erlfdb, "~> 0.3"},
      {:ecto, "~> 3.12"},
      {:jason, "~> 1.4"},
      {:credo, "~> 1.6", only: [:dev, :test, :docs]},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.16", only: :dev, runtime: false},
      {:benchee, "~> 1.0", only: :bench}
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

  defp docs_before_closing_head_tag(:html), do: docs_mermaid_js()
  defp docs_before_closing_head_tag(:epub), do: ""

  defp docs_before_closing_body_tag(:html), do: ""
  defp docs_before_closing_body_tag(:epub), do: ""

  defp docs_mermaid_js() do
    """
    <script>
      function mermaidLoaded() {
        mermaid.initialize({
          startOnLoad: false,
          theme: document.body.className.includes("dark") ? "dark" : "default"
        });
        let id = 0;
        for (const codeEl of document.querySelectorAll("pre code.mermaid")) {
          const preEl = codeEl.parentElement;
          const graphDefinition = codeEl.textContent;
          const graphEl = document.createElement("div");
          const graphId = "mermaid-graph-" + id++;
          mermaid.render(graphId, graphDefinition).then(({svg, bindFunctions}) => {
            graphEl.innerHTML = svg;
            bindFunctions?.(graphEl);
            preEl.insertAdjacentElement("afterend", graphEl);
            preEl.remove();
          });
        }
      }
    </script>
    <script async src="https://cdn.jsdelivr.net/npm/mermaid@10.2.3/dist/mermaid.min.js" onload="mermaidLoaded();"></script>
    """
  end
end
