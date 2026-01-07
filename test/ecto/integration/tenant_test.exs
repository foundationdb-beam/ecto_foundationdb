defmodule EctoIntegrationTenantTest do
  use Ecto.Integration.Case, async: true

  alias Ecto.Integration.TestRepo
  alias EctoFoundationDB.Tenant

  test "opening/creating tenants in parallel", context do
    id = context[:tenant_id]
    Tenant.clear_delete!(TestRepo, id)

    :ok =
      Task.async_stream(0..10, fn _ -> Tenant.open!(TestRepo, id) end) |> Stream.run()
  end
end
