defmodule Ecto.Integration.ClusterCase do
  @moduledoc false
  use ExUnit.CaseTemplate
  alias Ecto.Integration.TestRepo

  setup do
    repo_env = [migrator: nil]

    n = 3

    nodes =
      LocalCluster.start_nodes("ecto-foundationdb-migrations-test", n,
        applications: [],
        environment: [],
        files: Path.wildcard("test/support/**/*.ex")
      )

    nodes = List.zip([Enum.to_list(1..n), nodes])

    env_fn = fn x ->
      [
        bootstrap: [
          cluster:
            if(x > 1,
              do: :autojoin,
              else: [
                coordinator_addr: "127.0.0.1"
              ]
            ),
          conf: [
            data_dir: ".ex_fdbmonitor/#{x}/data",
            log_dir: ".ex_fdbmonitor/#{x}/log",
            fdbserver_ports: [5000 + x]
          ],
          fdbcli: if(x == 1, do: ~w[configure new single ssd tenant_mode=required_experimental]),
          fdbcli: if(x == 3, do: ~w[configure double]),
          fdbcli: if(x == n, do: ~w[coordinators auto])
        ],
        etc_dir: ".ex_fdbmonitor/#{x}/etc",
        run_dir: ".ex_fdbmonitor/#{x}/run"
      ]
    end

    bootstrap_fun = fn {idx, node} ->
      :ok = :rpc.call(node, Application, :load, [:ex_fdbmonitor])

      for {k, v} <- env_fn.(idx) do
        :ok = :rpc.call(node, Application, :put_env, [:ex_fdbmonitor, k, v])
      end

      {:ok, _} = :rpc.call(node, Application, :ensure_all_started, [:ex_fdbmonitor])
    end

    start_fun = fn {_idx, node} ->
      :rpc.call(node, Application, :load, [:ecto_foundationdb])
      :rpc.call(node, Application, :put_env, [:ecto_foundationdb, TestRepo, repo_env])
      {:ok, _} = :rpc.call(node, Application, :ensure_all_started, [:ecto_foundationdb])
      :ok = :rpc.call(node, EctoFoundationDB.Integration.ClusterHelper, :start, [TestRepo])
    end

    Enum.map(nodes, bootstrap_fun)

    Enum.map(nodes, start_fun)

    on_exit(fn ->
      Enum.map(nodes, fn {_idx, node} ->
        :rpc.call(node, TestRepo, :stop, [])
      end)

      :ok = LocalCluster.stop()

      Enum.map(nodes, fn {idx, _node} ->
        env = env_fn.(idx)
        etc_dir = env[:etc_dir]
        run_dir = env[:run_dir]
        data_dir = env[:bootstrap][:conf][:data_dir]
        log_dir = env[:bootstrap][:conf][:log_dir]

        for dir <- [etc_dir, run_dir, data_dir, log_dir] do
          File.rm_rf!(dir)
        end
      end)
    end)

    {:ok, [nodes: nodes]}
  end
end
