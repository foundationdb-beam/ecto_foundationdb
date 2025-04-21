Code.require_file("repo.exs", __DIR__)
Code.require_file("schemas.exs", __DIR__)

alias Ecto.Bench.FdbRepo
alias EctoFoundationDB.Tenant

{:ok, _} = Ecto.Adapters.FoundationDB.ensure_all_started(FdbRepo.config(), :temporary)

{:ok, _pid} = FdbRepo.start_link(log: false)

Tenant.open!(FdbRepo, "bench")
