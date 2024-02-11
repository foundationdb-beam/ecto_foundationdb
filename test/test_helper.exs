Logger.configure(level: :info)

Application.put_env(:ecto, :primary_key_type, :binary_id)
Application.put_env(:ecto, :async_integration_tests, false)

ecto = Mix.Project.deps_paths()[:ecto]
Code.require_file("#{ecto}/integration_test/support/schemas.exs", __DIR__)

alias Ecto.Integration.TestRepo

Application.put_env(:ecto_foundationdb, TestRepo,
  key_delimiter: "/",
  open_db: &Ecto.Adapters.FoundationDB.Sandbox.open_db/0,
  storage_tenant: Ecto.Adapters.FoundationDB.Sandbox
)

defmodule Ecto.Integration.Case do
  use ExUnit.CaseTemplate

  alias Ecto.Adapters.FoundationDB.Sandbox

  setup do
    tenants = Sandbox.checkout(TestRepo)
    {tenant_id, tenant} = tenants[:tenant]
    {other_tenant_id, other_tenant} = tenants[:other_tenant]
    on_exit(fn -> Ecto.Adapters.FoundationDB.Sandbox.checkin(TestRepo) end)

    {:ok,
     tenant: tenant,
     tenant_id: tenant_id,
     other_tenant: other_tenant,
     other_tenant_id: other_tenant_id}
  end
end

_ = Ecto.Adapters.FoundationDB.storage_down(TestRepo.config())
:ok = Ecto.Adapters.FoundationDB.storage_up(TestRepo.config())

{:ok, _} = TestRepo.start_link()

#:ok = Ecto.Migrator.up(TestRepo, 0, EctoFoundationDB.Integration.Migration, log: false)

ExUnit.start()
