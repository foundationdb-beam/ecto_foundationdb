Logger.configure(level: :info)

Application.put_env(:ecto, :primary_key_type, :binary_id)
Application.put_env(:ecto, :async_integration_tests, false)

ecto = Mix.Project.deps_paths()[:ecto]
Code.require_file("#{ecto}/integration_test/support/schemas.exs", __DIR__)

alias Ecto.Integration.TestRepo

Application.put_env(:ecto_foundationdb, TestRepo,
  open_db: &Ecto.Adapters.FoundationDB.Sandbox.open_db/0,
  storage_id: Ecto.Adapters.FoundationDB.Sandbox,

  # Increases number of index collisions
  indexkey_encoder: &Ecto.Adapters.FoundationDB.Layer.Pack.indexkey_encoder(&1, 1, &2),
  migrator: EctoFoundationDB.Integration.TestMigrator
)

defmodule Ecto.Integration.Case do
  @moduledoc false
  use ExUnit.CaseTemplate

  alias Ecto.Adapters.FoundationDB.Sandbox

  setup do
    tenant1 = Ecto.UUID.autogenerate()
    tenant2 = Ecto.UUID.autogenerate()

    tenant_task =
      Task.async(fn ->
        Sandbox.checkout(TestRepo, tenant1)
      end)

    other_tenant_task =
      Task.async(fn ->
        Sandbox.checkout(TestRepo, tenant2)
      end)

    tenant = Task.await(tenant_task)
    other_tenant = Task.await(other_tenant_task)

    on_exit(fn ->
      t1 = Task.async(fn -> Sandbox.checkin(TestRepo, tenant1) end)

      t2 =
        Task.async(fn -> Sandbox.checkin(TestRepo, tenant2) end)

      Task.await_many([t1, t2])
    end)

    {:ok,
     tenant: tenant, tenant_id: tenant1, other_tenant: other_tenant, other_tenant_id: tenant2}
  end
end

_ = Ecto.Adapters.FoundationDB.storage_down(TestRepo.config())
:ok = Ecto.Adapters.FoundationDB.storage_up(TestRepo.config())

{:ok, _} = TestRepo.start_link()

ExUnit.start()
