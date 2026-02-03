defmodule Ecto.Integration.MaxValueSizeTest do
  use Ecto.Integration.Case, async: true

  alias Ecto.Adapters.FoundationDB

  alias Ecto.Integration.TestRepo

  alias EctoFoundationDB.Future
  alias EctoFoundationDB.Schemas.User
  alias EctoFoundationDB.Test.Util

  import Ecto.Query

  describe "insert" do
    test "fdb:value_too_large", %{tenant: tenant} do
      # value_too_large - 2103 - Value length exceeds limit
      assert_raise(ErlangError, ~r/Erlang error: {:erlfdb_error, 2103}/, fn ->
        %User{name: "John", notes: Util.get_random_bytes(100_000)}
        |> FoundationDB.usetenant(tenant)
        |> TestRepo.insert(max_single_value_size: :infinity)
      end)
    end

    test "ecto_fdb:value_too_large", %{tenant: tenant} do
      assert_raise(ArgumentError, ~r/reject any objects larger than 100000 bytes/, fn ->
        %User{name: "John", notes: Util.get_random_bytes(100_000)}
        |> FoundationDB.usetenant(tenant)
        |> TestRepo.insert(max_value_size: 100_000)
      end)
    end

    test "split 1", %{tenant: tenant} do
      notes = Util.get_random_bytes(100_000)

      assert {:ok, user} =
               %User{name: "John", notes: notes}
               |> FoundationDB.usetenant(tenant)
               |> TestRepo.insert(max_single_value_size: 100_000)

      assert %User{} = TestRepo.get(User, user.id, prefix: tenant)

      future = TestRepo.watch(user, label: :max_value_watch)

      changeset = User.changeset(user, %{name: "Bob"})
      {:ok, changed} = TestRepo.update(changeset)

      assert changed.name == "Bob"

      # Simple wait for watch
      future_ref = Future.ref(future)

      receive do
        {^future_ref, :ready} ->
          :ok
      after
        100 ->
          raise "Watch failure"
      end

      assert %User{notes: ^notes} = TestRepo.get(User, user.id, prefix: tenant)

      assert %User{notes: ^notes} = TestRepo.get_by(User, [name: "Bob"], prefix: tenant)

      assert {:ok, _} = TestRepo.delete(user)

      assert is_nil(TestRepo.get(User, user.id, prefix: tenant))
    end

    test "reverse", %{tenant: tenant} do
      assert {:ok, _alice} =
               %User{id: "alice", name: "Alice", notes: Util.get_random_bytes(100_000)}
               |> FoundationDB.usetenant(tenant)
               |> TestRepo.insert(max_single_value_size: 100_000)

      assert {:ok, _bob} =
               %User{id: "bob", name: "Bob", notes: Util.get_random_bytes(100_000)}
               |> FoundationDB.usetenant(tenant)
               |> TestRepo.insert(max_single_value_size: 100_000)

      assert [%{id: "alice"}, %{id: "bob"}] =
               TestRepo.all(from(u in User, order_by: [asc: u.id]), prefix: tenant)

      assert [%{id: "bob"}, %{id: "alice"}] =
               TestRepo.all(from(u in User, order_by: [desc: u.id]), prefix: tenant)
    end
  end
end
