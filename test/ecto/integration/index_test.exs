defmodule Ecto.Integration.IndexTest do
  use Ecto.Integration.Case, async: true

  alias Ecto.Integration.TestRepo
  alias EctoFoundationDB.Schemas.User
  alias Ecto.Adapters.FoundationDB
  alias Ecto.Adapters.FoundationDB.Options
  alias Ecto.Adapters.FoundationDB.Exception.Unsupported

  import Ecto.Query

  @moduletag :integration
  describe "user index" do
    test "index consistency", context do
      tenant = context[:tenant]

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

      assert [%User{name: "John"}, %User{name: "John"}] =
               from(u in User, where: u.name == ^"John")
               |> TestRepo.all(prefix: tenant)

      # Forced Collision
      hash_1 = "7925D7E6AF6D09F0"
      hash_2 = "9AB33581B7409263"
      encoder = Options.get(TestRepo.config(), :indexkey_encoder)
      assert encoder.(hash_1, []) == encoder.(hash_2, [])

      {:ok, _user_hash_1} =
        %User{name: hash_1}
        |> FoundationDB.usetenant(tenant)
        |> TestRepo.insert()

      {:ok, _user_hash_2} =
        %User{name: hash_2}
        |> FoundationDB.usetenant(tenant)
        |> TestRepo.insert()

      assert [%User{name: ^hash_1}] =
               from(u in User, where: u.name == ^hash_1)
               |> TestRepo.all(prefix: tenant)
    end

    test "update_all via index", context do
      tenant = context[:tenant]

      {:ok, %User{id: user1_id}} =
        %User{name: "John"}
        |> FoundationDB.usetenant(tenant)
        |> TestRepo.insert()

      {:ok, _user2} =
        %User{name: "James"}
        |> FoundationDB.usetenant(tenant)
        |> TestRepo.insert()

      # assert [%User{id: ^user1_id, name: "Jane"}] =
      #         from(u in User, where: u.name == ^"John")
      #         |> TestRepo.update_all([set: [name: "Jane"]], prefix: tenant)
      assert_raise(Unsupported, fn ->
        from(u in User, where: u.name == ^"John")
        |> TestRepo.update_all([set: [name: "Jane"]], prefix: tenant)
      end)
    end

    test "between query fails on value index", context do
      tenant = context[:tenant]

      {:ok, _user1} =
        %User{name: "John"}
        |> FoundationDB.usetenant(tenant)
        |> TestRepo.insert()

      {:ok, _user2} =
        %User{name: "James"}
        |> FoundationDB.usetenant(tenant)
        |> TestRepo.insert()

      assert_raise(Unsupported, ~r/does not support 'between' queries/, fn ->
        from(u in User,
          where:
            u.name > ^"J" and
              u.name < ^"K"
        )
        |> TestRepo.all(prefix: tenant)
      end)
    end
  end
end
