Code.require_file("../support/setup.exs", __DIR__)

alias EctoFoundationDB.Tenant
alias Ecto.Bench.FDBRepo
alias Ecto.Bench.User

FDBRepo.start_link(log: false)

Tenant.clear_delete!(FDBRepo, "EctoFoundationDB.Bench")
tenant = Tenant.open_empty!(FDBRepo, "EctoFoundationDB.Bench", [])

inputs = %{
  "Struct" => fn -> User.sample_data() end
}

n = 4000

jobs = %{
  "FDB Repo.insert_all" => fn entry ->
    FDBRepo.insert_all(User, (for _ <- 1..n, do: entry.()), prefix: tenant, conflict_target: nil)
  end,
  "FDB Repo.insert_all no conflict" => fn entry ->
    FDBRepo.insert_all(User, (for _ <- 1..n, do: entry.()), prefix: tenant, conflict_target: [])
  end
}

# path = System.get_env("BENCHMARKS_OUTPUT_PATH") || "bench/results"

Benchee.run(
  jobs,
  inputs: inputs,
  formatters: [
    Benchee.Formatters.Console
  ]
)

FDBRepo.delete_all(User, prefix: tenant)
