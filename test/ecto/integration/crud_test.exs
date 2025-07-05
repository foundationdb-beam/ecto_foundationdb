defmodule Ecto.Integration.CrudTest do
  use Ecto.Integration.Case, async: true

  alias Ecto.Integration.TestRepo

  alias EctoFoundationDB.Schemas.Account
  alias EctoFoundationDB.Schemas.Product
  alias EctoFoundationDB.Schemas.User

  alias Ecto.Adapters.FoundationDB

  alias EctoFoundationDB.Exception.IncorrectTenancy
  alias EctoFoundationDB.Exception.Unsupported

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

    test "double insert", context do
      tenant = context[:tenant]

      {:ok, user} =
        %User{name: "John"}
        |> FoundationDB.usetenant(tenant)
        |> TestRepo.insert()

      assert_raise(Unsupported, ~r/Key exists/, fn -> TestRepo.insert(user) end)
    end

    test "insert fail, missing tenancy" do
      assert_raise(IncorrectTenancy, ~r/nil prefix was provided/, fn ->
        TestRepo.insert(%User{name: "George"})
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
        TestRepo.transactional(
          other_tenant,
          fn ->
            TestRepo.insert(user)
          end
        )
      end)

      # Here are 2 ways to force a struct into a different tenant's transaction
      assert :ok =
               TestRepo.transactional(
                 other_tenant,
                 fn ->
                   # Specify the equivalent tenant
                   %User{user | id: nil}
                   |> FoundationDB.usetenant(other_tenant)
                   |> TestRepo.insert()

                   # Remove the tenant from the struct, allow the tranction context to take over
                   %User{user | id: nil}
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

    test "get fail, 'where name ==' clause", context do
      tenant = context[:tenant]

      assert_raise(Unsupported, ~r/FoundationDB Adapter supports either/, fn ->
        query = from(u in User, where: u.inserted_at == ^~N[2999-01-01 00:00:00])
        TestRepo.all(query, prefix: tenant)
      end)
    end

    test "get fail, other queries", context do
      tenant = context[:tenant]

      assert_raise(
        Unsupported,
        ~r/FoundationDB Adapter has not implemented support for your query/,
        fn ->
          query = from(u in User, where: u.id != ^"foo")
          TestRepo.all(query, prefix: tenant)
        end
      )
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

      # Operations inside a FoundationDB Adapter Transaction have the tenant applied
      # automatically.
      {_ins_user, get_user} =
        TestRepo.transactional(
          tenant,
          fn ->
            {:ok, jesse} =
              %User{name: "Jesse"}
              |> TestRepo.insert()

            {:ok, _} =
              %User{name: "Sarah"}
              |> TestRepo.insert()

            {jesse, TestRepo.get(User, jesse.id)}
          end
        )

      # @todo: structs from inside a transaction don't inherit the prefix
      # assert ins_user.__meta__.prefix != nil
      assert get_user.name == "Jesse"
    end
  end

  describe "query" do
    test "from source", context do
      tenant = context[:tenant]

      names = ~w/John James Jesse Sarah Bob Steve/

      TestRepo.transactional(
        tenant,
        fn ->
          for n <- names do
            TestRepo.insert(%User{name: n})
          end
        end
      )

      assert [
               ["Bob", _],
               ["James", _],
               ["Jesse", _],
               ["John", _],
               ["Sarah", _],
               ["Steve", _]
             ] =
               from(u in "users", select: [u.name, u.id])
               |> TestRepo.all(prefix: tenant)
               |> Enum.sort()

      assert [_, _] =
               from(u in "users", select: [u.name, u.id])
               |> TestRepo.all(prefix: tenant, limit: 2)
               |> Enum.sort()
    end

    test "between pks with Ecto.Query", context do
      tenant = context[:tenant]

      TestRepo.insert(%User{id: "0001", name: "Alice"}, prefix: tenant)
      TestRepo.insert(%User{id: "0002", name: "Bob"}, prefix: tenant)
      TestRepo.insert(%User{id: "0003", name: "Charlie"}, prefix: tenant)

      assert [%User{id: "0001", name: "Alice"}, %User{id: "0002", name: "Bob"}] =
               from(u in User, where: u.id >= ^"0001" and u.id <= ^"0002")
               |> TestRepo.all(prefix: tenant)

      assert [%User{id: "0001"}, %User{id: "0002"}, %User{id: "0003"}] =
               from(u in User, where: u.id >= ^"00" and u.id < ^"1")
               |> TestRepo.all(prefix: tenant)
    end

    test "between pks with all_range", context do
      tenant = context[:tenant]

      TestRepo.insert(%User{id: "0001", name: "Alice"}, prefix: tenant)
      TestRepo.insert(%User{id: "0002", name: "Bob"}, prefix: tenant)
      TestRepo.insert(%User{id: "0003", name: "Charlie"}, prefix: tenant)

      assert [%User{id: "0001", name: "Alice"}, %User{id: "0002", name: "Bob"}] =
               TestRepo.all_range(User, "0001", "0003", prefix: tenant)

      assert [%{id: "0002", name: "Bob"}] =
               TestRepo.all_range("users", "0001", "0003", inclusive_left?: false, prefix: tenant)

      assert [[id: "0001", name: "Alice"], [id: "0002", name: "Bob"]] =
               TestRepo.all_range(from("users", select: [:id, :name]), "0001", "0002",
                 inclusive_right?: true,
                 prefix: tenant
               )
               |> Enum.map(&Map.to_list/1)

      assert [[id: "0001", name: "Alice"]] =
               TestRepo.all_range(from("users", select: [:id, :name]), "0001", "0002",
                 inclusive_right?: true,
                 prefix: tenant,
                 limit: 1
               )
               |> Enum.map(&Map.to_list/1)
    end

    test "single inequality pks with Ecto.Query", context do
      tenant = context[:tenant]

      TestRepo.insert(%User{id: "0001", name: "Alice"}, prefix: tenant)
      TestRepo.insert(%User{id: "0002", name: "Bob"}, prefix: tenant)
      TestRepo.insert(%User{id: "0003", name: "Charlie"}, prefix: tenant)

      assert [%User{id: "0002", name: "Bob"}, %User{id: "0003", name: "Charlie"}] =
               from(u in User, where: u.id > ^"0001")
               |> TestRepo.all(prefix: tenant)

      assert [%User{id: "0001", name: "Alice"}, %User{id: "0002", name: "Bob"}] =
               from(u in User, where: u.id <= ^"0002")
               |> TestRepo.all(prefix: tenant)
    end

    test "stream all", context do
      tenant = context[:tenant]

      names = ~w/John James Jesse Sarah Bob Steve/

      TestRepo.transactional(
        tenant,
        fn ->
          for n <- names do
            TestRepo.insert(%User{name: n})
          end
        end
      )

      # Each chunk of the stream is retrieved in a separate FDB transaction
      stream = TestRepo.stream(User, prefix: tenant, max_rows: 2)
      all_users = Enum.to_list(stream)
      assert length(all_users) == length(names)
    end
  end

  describe "delete" do
    test "deletes users", context do
      tenant = context[:tenant]
      {:ok, user} = TestRepo.insert(%User{name: "John"}, prefix: tenant)
      {:ok, _} = TestRepo.delete(user)
      assert TestRepo.get(User, user.id, prefix: tenant) == nil
    end

    test "delete something that doesn't exist", context do
      tenant = context[:tenant]

      assert_raise(Ecto.StaleEntryError, fn ->
        TestRepo.delete(%User{id: "doesnotexist"}, prefix: tenant)
      end)
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

    test "delete fail, missing tenancy" do
      assert_raise(IncorrectTenancy, ~r/nil prefix was provided/, fn ->
        TestRepo.delete(%User{id: "something", name: "George"})
      end)
    end
  end

  describe "update" do
    test "updates user", context do
      tenant = context[:tenant]
      {:ok, user} = TestRepo.insert(%User{name: "John"}, prefix: tenant)
      changeset = User.changeset(user, %{name: "Bob"})
      {:ok, changed} = TestRepo.update(changeset)

      assert changed.name == "Bob"
    end

    test "update fail, dne", context do
      tenant = context[:tenant]

      assert_raise(Ecto.StaleEntryError, fn ->
        %User{id: "doesnotexist", name: "George"}
        |> FoundationDB.usetenant(tenant)
        |> User.changeset(%{name: "Bob"})
        |> TestRepo.update()
      end)
    end

    test "update fail, missing tenancy" do
      assert_raise(IncorrectTenancy, ~r/nil prefix was provided/, fn ->
        %User{id: "something", name: "George"}
        |> User.changeset(%{name: "Bob"})
        |> TestRepo.update()
      end)
    end
  end
end
