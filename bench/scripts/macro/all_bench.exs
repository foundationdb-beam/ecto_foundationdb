# -----------------------------------Goal--------------------------------------
# Compare the performance of querying all objects of the different supported
# databases

# -------------------------------Description-----------------------------------
# This benchmark tracks performance of querying a set of objects registered in
# the database with Repo.all/2 function. The query pass through
# the steps of translating the SQL statements, sending them to the database and
# load the results into Ecto structures. Both, Ecto Adapters and Database itself
# play a role and can affect the results of this benchmark.

# ----------------------------Factors(don't change)---------------------------
# Different adapters supported by Ecto with the proper database up and running

# ----------------------------Parameters(change)-------------------------------
# There is only a unique parameter in this benchmark, the User objects to be
# fetched.

Code.require_file("../../support/setup.exs", __DIR__)

alias Ecto.Bench.User
alias EctoFoundationDB.Tenant

limit = 5_000

tenant = Tenant.open!(Ecto.Bench.FdbRepo, "bench")

users =
  1..limit
  |> Enum.map(fn _ -> User.sample_data() end)

# We need to insert data to fetch
f = Ecto.Bench.FdbRepo.transactional(tenant, fn ->
  Ecto.Bench.FdbRepo.async_insert_all!(User, users)
end)
Ecto.Bench.FdbRepo.await(f)

jobs = %{
  "Fdb Repo.all/2" => fn -> Ecto.Bench.FdbRepo.all(User, limit: limit, prefix: tenant) end
}

path = System.get_env("BENCHMARKS_OUTPUT_PATH") || "bench/results"
_file = Path.join(path, "all.json")

Benchee.run(
  jobs,
  formatters: [Benchee.Formatters.Console],
  time: 10,
  after_each: fn results ->
    ^limit = length(results)
  end
)

# Clean inserted data
Ecto.Bench.FdbRepo.delete_all(User, prefix: tenant)
