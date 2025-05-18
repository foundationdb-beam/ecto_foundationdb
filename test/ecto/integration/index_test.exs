defmodule Ecto.Integration.IndexTest do
  use Ecto.Integration.Case, async: true

  alias Ecto.Integration.TestRepo

  alias EctoFoundationDB.Schemas.User

  alias Ecto.Adapters.FoundationDB

  import Ecto.Query

  @moduletag :integration
  describe "user index" do
    test "index consistency", context do
      tenant = context[:tenant]

      assert tenant.backend == EctoFoundationDB.Tenant.DirectoryTenant

      # Insert consistency
      {:ok, _user1} =
        %User{name: "John"}
        |> FoundationDB.usetenant(tenant)
        |> TestRepo.insert()

      {:ok, user2} =
        %User{name: "James"}
        |> FoundationDB.usetenant(tenant)
        |> TestRepo.insert()

      {:ok, user3} =
        %User{name: "John"}
        |> FoundationDB.usetenant(tenant)
        |> TestRepo.insert()

      assert [%User{name: "John"}, %User{name: "John"}] =
               from(u in User, where: u.name == ^"John")
               |> TestRepo.all(prefix: tenant)

      # Delete consistency
      TestRepo.delete!(user3)

      assert [%User{name: "John"}] =
               from(u in User, where: u.name == ^"John")
               |> TestRepo.all(prefix: tenant)

      # Update consistency
      user2
      |> User.changeset(%{name: "John"})
      |> TestRepo.update!()

      assert nil == TestRepo.get_by(User, [name: "James"], prefix: tenant)

      assert [%User{name: "John"}, %User{name: "John"}] =
               from(u in User, where: u.name == ^"John")
               |> TestRepo.all(prefix: tenant)
    end

    test "update_all via index", context do
      tenant = context[:tenant]

      {:ok, user} =
        %User{name: "John"}
        |> FoundationDB.usetenant(tenant)
        |> TestRepo.insert()

      assert {1, _} =
               from(u in User, where: u.name == ^"John")
               |> TestRepo.update_all([set: [name: "Jane"]], prefix: tenant)

      user_id = user.id
      assert %User{id: ^user_id, name: "Jane"} = TestRepo.get!(User, user_id, prefix: tenant)
    end

    test "delete_all via index", context do
      tenant = context[:tenant]

      {:ok, user} =
        %User{name: "John"}
        |> FoundationDB.usetenant(tenant)
        |> TestRepo.insert()

      assert {1, _} =
               from(u in User, where: u.name == ^"John")
               |> TestRepo.delete_all(prefix: tenant)

      assert nil == TestRepo.get(User, user.id, prefix: tenant)
    end

    test "between query on string param", context do
      tenant = context[:tenant]

      {:ok, _user1} =
        %User{name: "John"}
        |> FoundationDB.usetenant(tenant)
        |> TestRepo.insert()

      {:ok, _user2} =
        %User{name: "James"}
        |> FoundationDB.usetenant(tenant)
        |> TestRepo.insert()

      assert [%User{name: "James"}] =
               from(u in User,
                 where:
                   u.name > "Ja" and
                     u.name < "Jo"
               )
               |> TestRepo.all(prefix: tenant)
    end

    test "greater/lesser query on string param", context do
      tenant = context[:tenant]

      {:ok, _user1} =
        %User{name: "John"}
        |> FoundationDB.usetenant(tenant)
        |> TestRepo.insert()

      {:ok, _user2} =
        %User{name: "James"}
        |> FoundationDB.usetenant(tenant)
        |> TestRepo.insert()

      assert [%User{name: "James"}, %User{name: "John"}] =
               from(u in User,
                 where: u.name > "Ja"
               )
               |> TestRepo.all(prefix: tenant)

      assert [%User{name: "James"}] =
               from(u in User,
                 where: u.name < "Jo"
               )
               |> TestRepo.all(prefix: tenant)
    end
  end
end
