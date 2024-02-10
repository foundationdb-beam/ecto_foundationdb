defmodule Ecto.Integration.CrudTest do
  use Ecto.Integration.Case

  alias Ecto.Integration.TestRepo
  alias EctoFoundationDB.Schemas.User
  alias EctoFoundationDB.Schemas.Account
  alias EctoFoundationDB.Schemas.Product
  alias EctoFoundationDB.Schemas.Global
  alias Ecto.Adapters.FoundationDB
  alias Ecto.Adapters.FoundationDB.Transaction
  alias Ecto.Adapters.FoundationDB.Exception.IncorrectTenancy
  alias Ecto.Adapters.FoundationDB.Exception.Unsupported

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

    test "insert fail, missing tenancy" do
      assert_raise(IncorrectTenancy, ~r/nil prefix was provided/, fn ->
        TestRepo.insert(%User{name: "George"})
      end)
    end

    test "insert fail, unused tenancy", context do
      assert_raise(IncorrectTenancy, ~r/non-nil prefix was provided/, fn ->
        TestRepo.insert(%Global{name: "failure"}, prefix: context[:tenant])
      end)
    end

    test "insert fail, tenancy only" do
      assert_raise(Unsupported, ~r/Non-tenant transactions/, fn ->
        TestRepo.insert(%Global{name: "failure"})
      end)
    end

    test "insert fail, cross tenancy transaction", context do
      tenant = context[:tenant]
      other_tenant = context[:other_tenant]
      {:ok, user} = TestRepo.insert(%User{name: "John"}, prefix: tenant)

      # Crossing a struct into another tenant is allowed when using Repo functions.
      assert {:ok, _} = TestRepo.insert(user, prefix: other_tenant)

      # Crossing a struct into another tenant is not allowed when using a transaction.
      assert_raise(IncorrectTenancy, ~r/original transaction context .* did not match/, fn ->
        Transaction.commit(
          other_tenant,
          fn ->
            TestRepo.insert(user)
          end
        )
      end)

      # Here are 2 ways to force a struct into a different tenant's transaction
      assert :ok =
               Transaction.commit(
                 other_tenant,
                 fn ->

                  # Specify the equivalent tenant
                   user
                   |> FoundationDB.usetenant(other_tenant)
                   |> TestRepo.insert()

                   # Remove the tenant from the struct, allow the tranction context to take over
                   user
                   |> FoundationDB.usetenant(nil)
                   |> TestRepo.insert()

                   :ok
                 end
               )
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

    test "get fail, missing tenancy" do
      assert_raise(IncorrectTenancy, ~r/nil prefix was provided/, fn ->
        TestRepo.get(Product, "abc123")
      end)
    end

    test "get fail, unused tenancy", context do
      assert_raise(IncorrectTenancy, ~r/nil prefix was provided/, fn ->
        TestRepo.get(Global, "abc123", prefix: context[:tenant])
      end)
    end

    test "get fail, tenancy only" do
      assert_raise(Unsupported, ~r/Non-tenant transactions/, fn ->
        TestRepo.get(Global, "abc123")
      end)
    end

    test "get fail, 'where name ==' clause", context do
      tenant = context[:tenant]
      assert_raise(Unsupported, fn ->
        query = from(u in User, where: u.name == "John")
        TestRepo.all(query, prefix: tenant)
      end)
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

      [%{name: "Jane" <> _}, %{name: "John" <> _}] =
        from(Account, order_by: :name)
        |> TestRepo.all(prefix: tenant)
    end

    test "tx_insert", context do
      tenant = context[:tenant]

      # Operations inside a FoundationDB Adapater Transaction have the tenant applied
      # automatically.
      user =
        Transaction.commit(
          tenant,
          fn ->
            {:ok, jesse} =
              %User{name: "Jesse"}
              |> TestRepo.insert()

            {:ok, _} =
              %User{name: "Sarah"}
              |> TestRepo.insert()

            TestRepo.get(User, jesse.id)
          end
        )

      assert user.name == "Jesse"
    end
  end

  describe "delete" do
    test "deletes users", context do
      tenant = context[:tenant]
      {:ok, user} = TestRepo.insert(%User{name: "John"}, prefix: tenant)
      {:ok, _} = TestRepo.delete(user)
      assert TestRepo.get(User, user.id, prefix: tenant) == nil
    end

    test "delete_all users", context do
      tenant = context[:tenant]
      {:ok, _user1} = TestRepo.insert(%User{name: "John"}, prefix: tenant)
      {:ok, _user2} = TestRepo.insert(%User{name: "James"}, prefix: tenant)
      assert {total, _} = TestRepo.delete_all(User, prefix: tenant)
      assert total >= 2
    end

    test "delete_all fail, missing tenancy" do
      assert_raise(IncorrectTenancy, ~r/nil prefix was provided/, fn ->
        TestRepo.delete_all(User)
      end)
    end
  end
end
