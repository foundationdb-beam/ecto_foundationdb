Code.require_file("../support/setup.exs", __DIR__)

alias Ecto.Adapters.FoundationDB
alias Ecto.Adapters.FoundationDB.Tenant
alias Ecto.Bench.FDBRepo
alias Ecto.Bench.User

db = FoundationDB.db(FDBRepo)
tenant = Tenant.open_empty!(db, "Ecto.Adapters.FoundationDB.Bench", [])

limit = 1_000

users =
  1..limit
  |> Enum.map(fn _ -> User.sample_data() end)

FDBRepo.insert_all(User, users, prefix: tenant)

jobs = %{
  "FDB Repo.all/2" => fn -> FDBRepo.all(User, prefix: tenant, limit: limit) end
}

path = System.get_env("BENCHMARKS_OUTPUT_PATH") || "bench/results"

Benchee.run(
  jobs,
  formatters: [
    Benchee.Formatters.Console
  ],
  time: 10,
  after_each: fn results ->
    ^limit = length(results)
  end
)

FDBRepo.delete_all(User, prefix: tenant)
