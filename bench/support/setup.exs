Code.require_file("repo.exs", __DIR__)
Code.require_file("migrations.exs", __DIR__)
Code.require_file("schemas.exs", __DIR__)

alias Ecto.Bench.FDBRepo

Application.put_env(:ecto_foundationdb, FDBRepo, [])

{:ok, _} = Ecto.Adapters.FoundationDB.ensure_all_started(FDBRepo.config(), :temporary)

{:ok, _pid} = FDBRepo.start_link(log: false)
