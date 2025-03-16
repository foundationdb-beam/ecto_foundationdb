defmodule Ecto.Integration.ManagedTenantCase do
  @moduledoc false
  use ExUnit.CaseTemplate

  alias Ecto.Integration.TestManagedTenantRepo

  setup do
    {:ok, _} = TestManagedTenantRepo.start_link()

    context = TenantsForCase.setup(TestManagedTenantRepo, log: false)

    on_exit(fn ->
      TenantsForCase.exit(TestManagedTenantRepo, context[:tenant_id], context[:other_tenant_id])
    end)

    {:ok, context}
  end
end

defmodule Ecto.Integration.ManagedTenantTest do
  use Ecto.Integration.ManagedTenantCase, async: false

  alias Ecto.Adapters.FoundationDB
  alias Ecto.Integration.TestManagedTenantRepo
  alias EctoFoundationDB.Schemas.User

  import Ecto.Query

  test "managed tenant", context do
    tenant = context[:tenant]

    assert tenant.backend == EctoFoundationDB.Tenant.ManagedTenant

    # Insert consistency
    {:ok, _user1} =
      %User{name: "John"}
      |> FoundationDB.usetenant(tenant)
      |> TestManagedTenantRepo.insert()

    {:ok, user2} =
      %User{name: "James"}
      |> FoundationDB.usetenant(tenant)
      |> TestManagedTenantRepo.insert()

    {:ok, user3} =
      %User{name: "John"}
      |> FoundationDB.usetenant(tenant)
      |> TestManagedTenantRepo.insert()

    assert [%User{name: "John"}, %User{name: "John"}] =
             from(u in User, where: u.name == ^"John")
             |> TestManagedTenantRepo.all(prefix: tenant)

    # Delete consistency
    TestManagedTenantRepo.delete!(user3)

    assert [%User{name: "John"}] =
             from(u in User, where: u.name == ^"John")
             |> TestManagedTenantRepo.all(prefix: tenant)

    # Update consistency
    user2
    |> User.changeset(%{name: "John"})
    |> TestManagedTenantRepo.update!()

    assert [%User{name: "John"}, %User{name: "John"}] =
             from(u in User, where: u.name == ^"John")
             |> TestManagedTenantRepo.all(prefix: tenant)
  end
end
