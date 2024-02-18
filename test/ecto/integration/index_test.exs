defmodule Ecto.Integration.IndexTest do
  use Ecto.Integration.Case, async: true

  alias Ecto.Integration.TestRepo
  alias EctoFoundationDB.Schemas.User
  alias Ecto.Adapters.FoundationDB
  alias Ecto.Adapters.FoundationDB.Options

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
  end
end
