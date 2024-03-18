Logger.configure(level: :info)

alias Ecto.Integration.TestRepo

Application.put_env(:ecto_foundationdb, TestRepo,
  open_db: &Ecto.Adapters.FoundationDB.Sandbox.open_db/0,
  storage_id: Ecto.Adapters.FoundationDB.Sandbox,
  migrator: EctoFoundationDB.Integration.TestMigrator,

  # Increases number of index collisions
  indexkey_encoder: &Ecto.Adapters.FoundationDB.Layer.Indexer.Default.indexkey_encoder(&1, 1, &2)
)

defmodule TenantsForCase do
  alias Ecto.Adapters.FoundationDB.Sandbox

  def setup(options) do
    tenant1 = Ecto.UUID.autogenerate()
    tenant2 = Ecto.UUID.autogenerate()

    tenant_task =
      Task.async(fn ->
        Sandbox.checkout(TestRepo, tenant1, options)
      end)

    other_tenant_task =
      Task.async(fn ->
        Sandbox.checkout(TestRepo, tenant2, options)
      end)

    tenant = Task.await(tenant_task)
    other_tenant = Task.await(other_tenant_task)

    [tenant: tenant, tenant_id: tenant1, other_tenant: other_tenant, other_tenant_id: tenant2]
  end

  def exit(tenant1, tenant2) do
    t1 = Task.async(fn -> Sandbox.checkin(TestRepo, tenant1) end)

    t2 =
      Task.async(fn -> Sandbox.checkin(TestRepo, tenant2) end)

    Task.await_many([t1, t2])
  end
end

defmodule Ecto.Integration.Case do
  @moduledoc false
  use ExUnit.CaseTemplate

  setup do
    context = TenantsForCase.setup([])

    on_exit(fn ->
      TenantsForCase.exit(context[:tenant_id], context[:other_tenant_id])
    end)

    {:ok, context}
  end
end

defmodule Ecto.Integration.MigrationsCase do
  @moduledoc false
  use ExUnit.CaseTemplate

  setup do
    context = TenantsForCase.setup(migrator: nil)

    on_exit(fn ->
      TenantsForCase.exit(context[:tenant_id], context[:other_tenant_id])
    end)

    {:ok, context}
  end
end

_ = Ecto.Adapters.FoundationDB.storage_down(TestRepo.config())
:ok = Ecto.Adapters.FoundationDB.storage_up(TestRepo.config())

{:ok, _} = TestRepo.start_link()

ExUnit.start()
