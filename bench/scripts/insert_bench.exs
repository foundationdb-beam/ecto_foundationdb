Code.require_file("../support/setup.exs", __DIR__)

alias Ecto.Adapters.FoundationDB
alias Ecto.Adapters.FoundationDB.Tenant
alias Ecto.Bench.FDBRepo
alias Ecto.Bench.User

db = FoundationDB.db(FDBRepo)
tenant = Tenant.open_empty!(db, "Ecto.Adapters.FoundationDB.Bench", [])

inputs = %{
  "Struct" => struct(User, User.sample_data()),
  "Changeset" => User.changeset(User.sample_data())
}

jobs = %{
  "FDB Repo.insert!/1" => fn entry -> FDBRepo.insert(entry, prefix: tenant) end
}

path = System.get_env("BENCHMARKS_OUTPUT_PATH") || "bench/results"

Benchee.run(
  jobs,
  inputs: inputs,
  formatters: [
    Benchee.Formatters.Console
  ]
)

FDBRepo.delete_all(User, prefix: tenant)
