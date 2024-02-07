defmodule Ecto.Integration.CrudTest do
  use Ecto.Integration.Case

  alias Ecto.Integration.TestRepo
  alias EctoFoundationDB.Schemas.User
  alias EctoFoundationDB.Schemas.Account
  alias EctoFoundationDB.Schemas.Product
  alias Ecto.Adapters.FoundationDB

  import Ecto.Query

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

    test "handles nulls when querying correctly", context do
      tenant = context[:tenant]

      {:ok, _account} =
        %Account{name: "Something"}
        |> FoundationDB.usetenant(tenant)
        |> TestRepo.insert()

      {:ok, product} =
        %Product{
          name: "Thing",
          approved_at: nil
        }
        |> FoundationDB.usetenant(tenant)
        |> TestRepo.insert()

      found = TestRepo.get(Product, product.id, prefix: tenant)
      assert found.id == product.id
      assert found.approved_at == nil
      assert found.description == nil
      assert found.name == "Thing"
      assert found.tags == []
    end

    test "insert_all", context do
      tenant = context[:tenant]

      timestamp = NaiveDateTime.utc_now() |> NaiveDateTime.truncate(:second)

      account1 = %{
        name: "John's account",
        inserted_at: timestamp,
        updated_at: timestamp
      }

      account2 = %{
        name: "Jane's account",
        inserted_at: timestamp,
        updated_at: timestamp
      }

      {2, nil} = TestRepo.insert_all(Account, [account2, account1], prefix: tenant)
      [%{name: "Jane"<>_},%{name: "John"<>_}] =
        (from Account, order_by: :name)
        |> TestRepo.all(prefix: tenant)
    end
  end
end
