defmodule Ecto.Integration.CrudTest do
  use Ecto.Integration.Case

  alias Ecto.Integration.TestRepo
  alias EctoFoundationDB.Schemas.User
  alias Ecto.Adapters.FoundationDB

  @moduletag :integration
  describe "insert" do
    test "insert user", context do
      tenant = context[:tenant]

      {:ok, user1} =
        %User{name: "John"}
        |> FoundationDB.usetenant(tenant)
        |> TestRepo.insert()

      assert user1

      {:ok, user2} =
        %User{name: "James"}
        |> FoundationDB.usetenant(tenant)
        |> TestRepo.insert()

      assert user2

      assert user1.id != user2.id

      user = TestRepo.get(User, user1.id, prefix: tenant)

      assert user.name == "John"
    end
  end
end
