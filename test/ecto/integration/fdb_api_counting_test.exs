defmodule Ecto.Integration.FdbApiCountingTest do
  use Ecto.Integration.Case, async: false

  alias Ecto.Adapters.FoundationDB
  alias Ecto.Integration.TestRepo

  alias EctoFoundationDB.ModuleToModuleTracer
  alias EctoFoundationDB.Schemas.User
  alias EctoFoundationDB.Test.Util

  # This module tracks all calls from modules that begin with EctoFoundationDB.* to the following
  # list of :erlfdb exported functions. If there are new relevant functions to track, they must be
  # added here.
  #
  # By counting and documenting FDB API function calls, we can ensure new calls don't creep in unnoticed,
  # and we can provide some explanatory documentation about how the Layer works.
  @traced_calls [
    {:erlfdb, :get, 2},
    {:erlfdb, :get_range, 4},
    {:erlfdb, :wait, 1},
    {:erlfdb, :wait_for_any, 1},
    {:erlfdb, :set, 3},
    {:erlfdb, :clear, 2},
    {:erlfdb, :clear_range, 3},
    {:erlfdb, :max, 3},
    {:erlfdb, :fold_range, 5},
    {:erlfdb, :get_mapped_range, 5},
    {:erlfdb, :wait_for_all, 1},
    {:erlfdb, :watch, 2},
    {:erlfdb, :wait_for_all_interleaving, 2}
  ]

  def with_erlfdb_calls(fun) do
    caller_spec = fn caller_module ->
      try do
        case Module.split(caller_module) do
          ["EctoFoundationDB" | _] ->
            true

          _ ->
            false
        end
      rescue
        _e in ArgumentError ->
          false
      end
    end

    {traced_calls, res} =
      ModuleToModuleTracer.with_traced_calls([caller_spec], @traced_calls, fun)

    # clean up the traces for more concise assertions
    traced_calls =
      for {caller, {:erlfdb, call_fun, _arity}} <- traced_calls do
        {caller, call_fun}
      end

    {traced_calls, res}
  end

  test "counting", context do
    tenant = context[:tenant]

    # =================================================================
    # Insert (no index inventory cache)
    # =================================================================

    {calls, alice} =
      with_erlfdb_calls(fn ->
        {:ok, alice} =
          %User{name: "Alice"}
          |> FoundationDB.usetenant(tenant)
          |> TestRepo.insert()

        alice
      end)

    assert [
             # get max_version
             {EctoFoundationDB.Layer.IndexInventory, :get},

             # get source claim_key
             {EctoFoundationDB.Layer.IndexInventory, :get},

             # there is no cache, so get idxs
             {EctoFoundationDB.Layer.IndexInventory, :get_range},

             # wait for max_version, claim_key, and idxs range
             {EctoFoundationDB.Layer.IndexInventory, :wait_for_all_interleaving},

             # check for existence of primary write
             {EctoFoundationDB.Layer.KVZipper, :get_range},

             # wait for existence check
             {EctoFoundationDB.Future, :wait_for_all_interleaving},

             # primary write
             {EctoFoundationDB.Layer.TxInsert, :set},

             # index write
             {EctoFoundationDB.Indexer.Default, :set}
           ] == calls

    # =================================================================
    # Insert (with index inventory cache)
    # =================================================================

    {calls, _bob} =
      with_erlfdb_calls(fn ->
        {:ok, bob} =
          %User{name: "Bob"}
          |> FoundationDB.usetenant(tenant)
          |> TestRepo.insert()

        bob
      end)

    assert [
             # get max_version and claim_key. We have cached the inventory, so
             # these waits are deferred optimistically.
             {EctoFoundationDB.Layer.IndexInventory, :get},
             {EctoFoundationDB.Layer.IndexInventory, :get},

             # check for existence of primary write
             {EctoFoundationDB.Layer.KVZipper, :get_range},
             {EctoFoundationDB.Future, :wait_for_all_interleaving},

             # primary write, index write
             {EctoFoundationDB.Layer.TxInsert, :set},
             {EctoFoundationDB.Indexer.Default, :set},

             # wait for max_version and claim_key
             {EctoFoundationDB.Layer.IndexInventory, :wait_for_all}
           ] == calls

    # =================================================================
    # Get using primary id
    # =================================================================

    {calls, _} =
      with_erlfdb_calls(fn ->
        TestRepo.get(User, alice.id, prefix: tenant)
      end)

    assert [
             # get max_version and claim_key
             {EctoFoundationDB.Layer.IndexInventory, :get},
             {EctoFoundationDB.Layer.IndexInventory, :get},

             # get and wait primary write key
             {EctoFoundationDB.Layer.Query, :get_range},
             {EctoFoundationDB.Future, :wait_for_all_interleaving},

             # wait for max_version and claim_key
             {EctoFoundationDB.Layer.IndexInventory, :wait_for_all}
           ] = calls

    # =================================================================
    # Update an indexed field
    # =================================================================

    {calls, _} =
      with_erlfdb_calls(fn ->
        changeset = User.changeset(alice, %{name: "Alicia"})
        {:ok, _} = TestRepo.update(changeset)
      end)

    assert [
             # get max_version and claim_key
             {EctoFoundationDB.Layer.IndexInventory, :get},
             {EctoFoundationDB.Layer.IndexInventory, :get},

             # get and wait for existing data from primary write
             {EctoFoundationDB.Layer.KVZipper, :get_range},
             {EctoFoundationDB.Future, :wait_for_all_interleaving},

             # set data in primary write
             {EctoFoundationDB.Layer.Tx, :set},

             # clear and set default index. :name has changed
             {EctoFoundationDB.Indexer.Default, :clear},
             {EctoFoundationDB.Indexer.Default, :set},

             # wait for max_version and claim_key
             {EctoFoundationDB.Layer.IndexInventory, :wait_for_all}
           ] == calls

    # =================================================================
    # Update an non-indexed field
    # =================================================================

    {calls, _} =
      with_erlfdb_calls(fn ->
        changeset = User.changeset(alice, %{notes: "Hello world"})
        {:ok, _} = TestRepo.update(changeset)
      end)

    assert [
             # get max_version and claim_key
             {EctoFoundationDB.Layer.IndexInventory, :get},
             {EctoFoundationDB.Layer.IndexInventory, :get},

             # get and wait for existing data from primary write
             {EctoFoundationDB.Layer.KVZipper, :get_range},
             {EctoFoundationDB.Future, :wait_for_all_interleaving},

             # set data in primary write
             {EctoFoundationDB.Layer.Tx, :set},

             # wait for max_version and claim_key
             {EctoFoundationDB.Layer.IndexInventory, :wait_for_all}
           ] == calls

    # =================================================================
    # Get by indexed field
    # =================================================================

    {calls, _} =
      with_erlfdb_calls(fn ->
        TestRepo.get_by(User, [name: "Alicia"], prefix: tenant)
      end)

    assert [
             # get max_version and claim_key
             {EctoFoundationDB.Layer.IndexInventory, :get},
             {EctoFoundationDB.Layer.IndexInventory, :get},

             # get_mapped_range for :name index. The call to :get_range shown here
             # is a tail call from get_mapped_range, so it's expected and harmless
             {EctoFoundationDB.Layer.Query, :get_mapped_range},
             {EctoFoundationDB.Layer.Query, :get_range},

             # wait for get_mapped_range result
             {EctoFoundationDB.Future, :wait_for_all_interleaving},

             # wait for max_version and claim_key
             {EctoFoundationDB.Layer.IndexInventory, :wait_for_all}
           ] == calls

    # =================================================================
    # Async get by indexed field
    # =================================================================

    {calls, _} =
      with_erlfdb_calls(fn ->
        TestRepo.transaction(
          fn ->
            f1 = TestRepo.async_get_by(User, name: "Alicia")
            f2 = TestRepo.async_get_by(User, name: "Bob")
            TestRepo.await([f1, f2])
          end,
          prefix: tenant
        )
      end)

    assert [
             # get max_version and claim_key
             {EctoFoundationDB.Layer.IndexInventory, :get},
             {EctoFoundationDB.Layer.IndexInventory, :get},

             # get_mapped_range for :name index
             {EctoFoundationDB.Layer.Query, :get_mapped_range},
             {EctoFoundationDB.Layer.Query, :get_range},

             # wait for max_version and claim_key, @todo ideally this happens at the very end (#26)
             {EctoFoundationDB.Layer.IndexInventory, :wait_for_all},

             # get max_version and claim_key
             {EctoFoundationDB.Layer.IndexInventory, :get},
             {EctoFoundationDB.Layer.IndexInventory, :get},

             # get_mapped_range for :name index
             {EctoFoundationDB.Layer.Query, :get_mapped_range},
             {EctoFoundationDB.Layer.Query, :get_range},

             # max_version and claim_key again. FDB implements a RWY-transaction, so
             # this trivially reads from local memory
             {EctoFoundationDB.Layer.IndexInventory, :wait_for_all},

             # wait for results of both get_mapped_range calls
             {EctoFoundationDB.Future, :wait_for_all_interleaving}
           ] == calls

    # =================================================================
    # Delete
    # =================================================================

    {calls, _} =
      with_erlfdb_calls(fn ->
        {:ok, _} = TestRepo.delete(alice)
      end)

    assert [
             # get max_version and claim_key
             {EctoFoundationDB.Layer.IndexInventory, :get},
             {EctoFoundationDB.Layer.IndexInventory, :get},

             # check for existence
             {EctoFoundationDB.Layer.KVZipper, :get_range},
             {EctoFoundationDB.Future, :wait_for_all_interleaving},

             # clear primary
             {EctoFoundationDB.Layer.Tx, :clear},

             # clear :name index
             {EctoFoundationDB.Indexer.Default, :clear},

             # wait for max_version and claim_key
             {EctoFoundationDB.Layer.IndexInventory, :wait_for_all}
           ] == calls

    # =================================================================
    # Insert Unzipped Object
    # =================================================================

    {calls, eve} =
      with_erlfdb_calls(fn ->
        {:ok, alice} =
          %User{name: "Eve", notes: Util.get_random_bytes(100_000)}
          |> FoundationDB.usetenant(tenant)
          |> TestRepo.insert()

        alice
      end)

    assert [
             # get max_version
             {EctoFoundationDB.Layer.IndexInventory, :get},

             # get source claim_key
             {EctoFoundationDB.Layer.IndexInventory, :get},

             # check for existence of primary write
             {EctoFoundationDB.Layer.KVZipper, :get_range},

             # wait for existence check
             {EctoFoundationDB.Future, :wait_for_all_interleaving},

             # primary write
             {EctoFoundationDB.Layer.TxInsert, :set},

             # unzipped writes
             {EctoFoundationDB.Layer.TxInsert, :set},
             {EctoFoundationDB.Layer.TxInsert, :set},

             # index write
             {EctoFoundationDB.Indexer.Default, :set},

             # wait for max_version and claim_key
             {EctoFoundationDB.Layer.IndexInventory, :wait_for_all}
           ] == calls

    # =================================================================
    # Update an non-indexed field on a Zipped Object into a normal object
    # =================================================================

    {calls, eve} =
      with_erlfdb_calls(fn ->
        changeset = User.changeset(eve, %{notes: "Hello world"})
        {:ok, eve} = TestRepo.update(changeset)
        eve
      end)

    assert [
             # get max_version and claim_key
             {EctoFoundationDB.Layer.IndexInventory, :get},
             {EctoFoundationDB.Layer.IndexInventory, :get},

             # get and wait for existing data from primary write
             {EctoFoundationDB.Layer.KVZipper, :get_range},
             {EctoFoundationDB.Future, :wait_for_all_interleaving},

             # clear all existing unzipped keys
             {EctoFoundationDB.Layer.Tx, :clear_range},

             # set data in primary write
             {EctoFoundationDB.Layer.Tx, :set},

             # wait for max_version and claim_key
             {EctoFoundationDB.Layer.IndexInventory, :wait_for_all}
           ] == calls

    # =================================================================
    # Update an non-indexed field on an normal object into a zipped object
    # =================================================================

    {calls, eve} =
      with_erlfdb_calls(fn ->
        changeset = User.changeset(eve, %{notes: Util.get_random_bytes(100_000)})
        {:ok, eve} = TestRepo.update(changeset)
        eve
      end)

    assert [
             # get max_version and claim_key
             {EctoFoundationDB.Layer.IndexInventory, :get},
             {EctoFoundationDB.Layer.IndexInventory, :get},

             # get and wait for existing data from primary write
             {EctoFoundationDB.Layer.KVZipper, :get_range},
             {EctoFoundationDB.Future, :wait_for_all_interleaving},

             # set data in primary write
             {EctoFoundationDB.Layer.Tx, :set},

             # unzipped writes
             {EctoFoundationDB.Layer.Tx, :set},
             {EctoFoundationDB.Layer.Tx, :set},

             # wait for max_version and claim_key
             {EctoFoundationDB.Layer.IndexInventory, :wait_for_all}
           ] == calls

    # =================================================================
    # Delete a zipped object
    # =================================================================

    {calls, _eve} =
      with_erlfdb_calls(fn ->
        {:ok, _} = TestRepo.delete(eve)
      end)

    assert [
             # get max_version and claim_key
             {EctoFoundationDB.Layer.IndexInventory, :get},
             {EctoFoundationDB.Layer.IndexInventory, :get},

             # check for existence
             {EctoFoundationDB.Layer.KVZipper, :get_range},
             {EctoFoundationDB.Future, :wait_for_all_interleaving},

             # clear unzipped range
             {EctoFoundationDB.Layer.Tx, :clear_range},

             # clear :name index
             {EctoFoundationDB.Indexer.Default, :clear},

             # wait for max_version and claim_key
             {EctoFoundationDB.Layer.IndexInventory, :wait_for_all}
           ] == calls
  end
end
