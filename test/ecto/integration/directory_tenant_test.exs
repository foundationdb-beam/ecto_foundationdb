defmodule EctoIntegrationDirectoryTenantTest do
  use Ecto.Integration.Case, async: true

  alias Ecto.Integration.TestRepo
  alias EctoFoundationDB.Tenant

  test "list", %{tenant_id: id} do
    assert ids = Tenant.list(TestRepo)
    assert Enum.member?(ids, id)
  end
end
