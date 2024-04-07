Code.require_file("../support/setup.exs", __DIR__)

alias EctoFoundationDB.Tenant
alias Ecto.Bench.FDBRepo
alias Ecto.Bench.User

tenant = Tenant.open_empty!(FDBRepo, "EctoFoundationDB.Bench")

limit = 1_000

users =
  1..limit
  |> Enum.map(fn _ -> User.sample_data() end)

FDBRepo.insert_all(User, users, prefix: tenant)

test_user = Enum.random(users)

jobs = %{
  "FDB Repo.get/3" => fn _ ->
    FDBRepo.get(User, test_user.id, prefix: tenant)
  end,
  "FDB Repo.get_by/3" => fn _ ->
    FDBRepo.get_by(User, [uuid: test_user.uuid], prefix: tenant)
  end
}

# path = System.get_env("BENCHMARKS_OUTPUT_PATH") || "bench/results"

Benchee.run(
  jobs,
  formatters: [
    Benchee.Formatters.Console
  ],
  time: 5,
  inputs: %{
    "idx_cache enabled" => :enabled,
    "idx_cache disabled" => :disabled
  },
  before_scenario: fn input ->
    try do
      FDBRepo.stop()
    catch _, _ -> :ok end
    {:ok, _pid} = FDBRepo.start_link(log: false, idx_cache: input)
    {input, :ok}
  end
)

{:ok, _pid} = FDBRepo.start_link(log: false)
FDBRepo.delete_all(User, prefix: tenant)
