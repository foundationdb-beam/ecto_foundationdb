Logger.configure(level: :info)

:ok = FDB.Case.init()

alias Ecto.Integration.SingleTenantTestRepo
alias Ecto.Integration.TestManagedTenantRepo
alias Ecto.Integration.TestRepo
alias Ecto.Integration.TinyRepo

Application.put_env(:ecto_foundationdb, TestRepo,
  open_db: &EctoFoundationDB.Sandbox.open_db/1,
  storage_id: EctoFoundationDB.Sandbox,
  migrator: EctoFoundationDB.Integration.TestMigrator
)

Application.put_env(:ecto_foundationdb, TinyRepo,
  open_db: &EctoFoundationDB.Sandbox.open_db/1,
  storage_id: EctoFoundationDB.Sandbox
)

Application.put_env(:ecto_foundationdb, SingleTenantTestRepo,
  open_db: &EctoFoundationDB.Sandbox.open_db/1,
  storage_id: EctoFoundationDB.Sandbox,
  tenant_id: "single-tenant"
)

Application.put_env(:ecto_foundationdb, TestManagedTenantRepo,
  open_db: &EctoFoundationDB.Sandbox.open_db/1,
  storage_id: EctoFoundationDB.Sandbox,
  migrator: EctoFoundationDB.Integration.TestMigrator,
  tenant_backend: EctoFoundationDB.Tenant.ManagedTenant
)

Application.put_env(:ecto_foundationdb, CliTest.Repo,
  open_db: &EctoFoundationDB.Sandbox.open_db/1,
  storage_id: EctoFoundationDB.Sandbox,
  migrator: CliTest.Migrator
)

{:ok, _} = TestRepo.start_link()
{:ok, _} = TinyRepo.start_link()
{:ok, _} = SingleTenantTestRepo.start_link()
{:ok, _} = CliTest.Repo.start_link()

ExUnit.start()
