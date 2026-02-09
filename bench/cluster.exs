# Shared LocalCluster setup for benchmarks.
#
# Usage:
#
#   Code.require_file("bench/cluster.exs")
#   cluster_file = Bench.Cluster.start()
#   # ... configure Repo with cluster_file ...
#   Bench.Cluster.stop()

defmodule Bench.Cluster do
  @name "bench"
  @starting_port 5100

  def start(opts \\ []) do
    processes = Keyword.get(opts, :processes, 1)
    nodes = Keyword.get(opts, :nodes, 3)

    ExFdbmonitor.Sandbox.start()

    sandbox =
      ExFdbmonitor.Sandbox.Single.checkout(@name,
        starting_port: @starting_port,
        processes: processes,
        nodes: nodes
      )

    Process.put(:bench_sandbox, sandbox)

    ExFdbmonitor.Sandbox.cluster_file(@name, 0)
  end

  def stop() do
    sandbox = Process.get(:bench_sandbox)

    if sandbox do
      ExFdbmonitor.Sandbox.Single.checkin(sandbox, drop?: true)
      Process.delete(:bench_sandbox)
    end

    :ok
  end
end
