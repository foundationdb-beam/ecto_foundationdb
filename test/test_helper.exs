Logger.configure(level: :info)

alias Ecto.Integration.TestRepo
alias Ecto.Integration.TestManagedTenantRepo

Application.put_env(:ecto_foundationdb, TestRepo,
  open_db: &EctoFoundationDB.Sandbox.open_db/1,
  storage_id: EctoFoundationDB.Sandbox,
  migrator: EctoFoundationDB.Integration.TestMigrator
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
{:ok, _} = CliTest.Repo.start_link()

ExUnit.start()
