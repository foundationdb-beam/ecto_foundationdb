Code.require_file("../support/setup.exs", __DIR__)

alias EctoFoundationDB.Tenant
alias Ecto.Bench.FDBRepo
alias Ecto.Bench.User

FDBRepo.start_link(log: false)

tenant = Tenant.open_empty!(FDBRepo, "EctoFoundationDB.Bench", [])

inputs = %{
  "Struct" => fn -> struct(User, User.sample_data()) end,
  "Changeset" => fn -> User.changeset(User.sample_data()) end
}

jobs = %{
  "FDB Repo.insert!/1" => fn entry ->
    FDBRepo.insert(entry.(), prefix: tenant)
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
