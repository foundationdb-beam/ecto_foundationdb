defmodule TenantForCase do
  @moduledoc false
  alias EctoFoundationDB.Sandbox

  def setup(repo, options) do
    tenant_id = Ecto.UUID.autogenerate()

    tenant = Sandbox.checkout(repo, tenant_id, options)

    [tenant: tenant, tenant_id: tenant_id]
  end

  def exit(repo, tenant_id) do
    Sandbox.checkin(repo, tenant_id)
  end
end
