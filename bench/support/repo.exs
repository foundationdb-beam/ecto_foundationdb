fdb_bench_cluster_file = System.get_env("FDB_CLUSTER_FILE") || "/usr/local/etc/foundationdb/fdb.cluster"

Application.put_env(:ecto_foundationdb, Ecto.Bench.FdbRepo,
  cluster_file: fdb_bench_cluster_file,
  storage_id: EctoFoundationDB.Bench
)

defmodule Ecto.Bench.FdbRepo do
  use Ecto.Repo, otp_app: :ecto_foundationdb, adapter: Ecto.Adapters.FoundationDB, log: false
end
