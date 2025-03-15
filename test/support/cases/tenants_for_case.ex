defmodule TenantsForCase do
  @moduledoc false
  alias EctoFoundationDB.Sandbox

  def setup(repo, options) do
    tenant1 = Ecto.UUID.autogenerate()
    tenant2 = Ecto.UUID.autogenerate()

    tenant_task =
      Task.async(fn ->
        Sandbox.checkout(repo, tenant1, options)
      end)

    other_tenant_task =
      Task.async(fn ->
        Sandbox.checkout(repo, tenant2, options)
      end)

    tenant = Task.await(tenant_task)
    other_tenant = Task.await(other_tenant_task)

    [tenant: tenant, tenant_id: tenant1, other_tenant: other_tenant, other_tenant_id: tenant2]
  end

  def exit(repo, tenant1, tenant2) do
    t1 = Task.async(fn -> Sandbox.checkin(repo, tenant1) end)

    t2 =
      Task.async(fn -> Sandbox.checkin(repo, tenant2) end)

    Task.await_many([t1, t2])
  end
end
