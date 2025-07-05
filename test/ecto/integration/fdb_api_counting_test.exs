defmodule Ecto.Integration.FdbApiCountingTest do
  # Cannot be async because the global metadataVersion key is not isolated in tenants. Other
  # tests can interfere.
  use Ecto.Integration.Case, async: false

  alias EctoFoundationDB.Layer.MetadataVersion
  alias EctoFoundationDB.Tenant
  alias EctoFoundationDB.Versionstamp

  alias Ecto.Adapters.FoundationDB
  alias Ecto.Integration.TestRepo

  alias EctoFoundationDB.ModuleToModuleTracer
  alias EctoFoundationDB.Schemas.QueueItem
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
    {:erlfdb, :set_versionstamped_key, 3},
    {:erlfdb, :set_versionstamped_value, 3},
    {:erlfdb, :clear, 2},
    {:erlfdb, :clear_range, 3},
    {:erlfdb, :max, 3},
    {:erlfdb, :fold_range, 5},
    {:erlfdb, :get_mapped_range, 5},
    {:erlfdb, :wait_for_all, 1},
    {:erlfdb, :watch, 2},
    {:erlfdb, :wait_for_all_interleaving, 2}
  ]

  def with_erlfdb_calls(name, fun) do
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
      ModuleToModuleTracer.with_traced_calls(name, [caller_spec], @traced_calls, fun)

    # clean up the traces for more concise assertions
    traced_calls =
      for {caller, {:erlfdb, call_fun, _arity}} <- traced_calls do
        {caller, call_fun}
      end

    {traced_calls, res}
  end

  test "insert with no metadata cache", context do
    tenant = context[:tenant]

    {calls, _alice} =
      with_erlfdb_calls(context.test, fn ->
        {:ok, alice} =
          %User{name: "Alice"}
          |> FoundationDB.usetenant(tenant)
          |> TestRepo.insert()

        alice
      end)

    assert [
             # we always get global metadataVersion
             {EctoFoundationDB.Layer.MetadataVersion, :get},
             {EctoFoundationDB.Future, :wait},

             # there is no cache, so get app version, source claim key, and indexes
             {EctoFoundationDB.Layer.MetadataVersion, :get},
             {EctoFoundationDB.MigrationsPJ, :get},
             {EctoFoundationDB.Layer.Metadata, :get_range},
             {EctoFoundationDB.Future, :wait_for_all_interleaving},

             # check for existence of primary write
             {EctoFoundationDB.Layer.Tx, :get_range},
             {EctoFoundationDB.Future, :wait_for_all_interleaving},

             # primary write
             {EctoFoundationDB.Layer.PrimaryKVCodec, :set},

             # index write
             {EctoFoundationDB.Indexer.Default, :set}
           ] == calls
  end

  test "insert with global-validated metadata cache", context do
    tenant = context[:tenant]

    # Prime metadata cache
    _alice = TestRepo.insert!(%User{name: "Alice"}, prefix: tenant)

    {calls, _bob} =
      with_erlfdb_calls(context.test, fn ->
        {:ok, bob} =
          %User{name: "Bob"}
          |> FoundationDB.usetenant(tenant)
          |> TestRepo.insert()

        bob
      end)

    assert [
             # we always get global metadataVersion
             {EctoFoundationDB.Layer.MetadataVersion, :get},
             {EctoFoundationDB.Future, :wait},

             # cache is a match, so there's no work here! :)

             # check for existence of primary write
             {EctoFoundationDB.Layer.Tx, :get_range},
             {EctoFoundationDB.Future, :wait_for_all_interleaving},

             # primary write, index write
             {EctoFoundationDB.Layer.PrimaryKVCodec, :set},
             {EctoFoundationDB.Indexer.Default, :set}
           ] == calls
  end

  test "insert with local-validated metadata cache", context do
    tenant = context[:tenant]

    # Prime metadata cache
    _alice = TestRepo.insert!(%User{name: "Alice"}, prefix: tenant)

    # Update the global metadataVersion
    FoundationDB.transactional(tenant, &MetadataVersion.tx_set_global/1)

    {calls, _bob} =
      with_erlfdb_calls(context.test, fn ->
        {:ok, bob} =
          %User{name: "Bob"}
          |> FoundationDB.usetenant(tenant)
          |> TestRepo.insert()

        bob
      end)

    assert [
             # we always get global metadataVersion
             {EctoFoundationDB.Layer.MetadataVersion, :get},
             {EctoFoundationDB.Future, :wait},

             # global metadataVersion does not match, so we need to check
             # local (app version) and claim key
             {EctoFoundationDB.Layer.MetadataVersion, :get},
             {EctoFoundationDB.MigrationsPJ, :get},

             # check for existence of primary write
             {EctoFoundationDB.Layer.Tx, :get_range},
             {EctoFoundationDB.Future, :wait_for_all_interleaving},

             # primary write, index write
             {EctoFoundationDB.Layer.PrimaryKVCodec, :set},
             {EctoFoundationDB.Indexer.Default, :set},

             # wait for app metadata version and claim_key
             {EctoFoundationDB.Future, :wait_for_all_interleaving}
           ] == calls
  end

  test "insert with shared metadata cache across open tenants", context do
    tenant = context[:tenant]
    # Prime metadata cache
    _alice = TestRepo.insert!(%User{name: "Alice"}, prefix: tenant)

    newly_opened_tenant = Tenant.open(TestRepo, tenant.id, log: false)

    {calls, _bob} =
      with_erlfdb_calls(context.test, fn ->
        {:ok, bob} =
          %User{name: "Charlie"}
          |> FoundationDB.usetenant(newly_opened_tenant)
          |> TestRepo.insert()

        bob
      end)

    assert [
             # we always get global metadataVersion
             {EctoFoundationDB.Layer.MetadataVersion, :get},
             {EctoFoundationDB.Future, :wait},

             # check for existence of primary write
             {EctoFoundationDB.Layer.Tx, :get_range},
             {EctoFoundationDB.Future, :wait_for_all_interleaving},

             # primary write, index write
             {EctoFoundationDB.Layer.PrimaryKVCodec, :set},
             {EctoFoundationDB.Indexer.Default, :set}
           ] == calls
  end

  test "get using primary id", context do
    tenant = context[:tenant]
    alice = TestRepo.insert!(%User{name: "Alice"}, prefix: tenant)

    {calls, _} =
      with_erlfdb_calls(context.test, fn ->
        TestRepo.get(User, alice.id, prefix: tenant)
      end)

    assert [
             # we always get global metadataVersion
             {EctoFoundationDB.Layer.MetadataVersion, :get},
             {EctoFoundationDB.Future, :wait},

             # get and wait primary write key
             {EctoFoundationDB.Layer.Query, :get_range},
             {EctoFoundationDB.Future, :wait_for_all_interleaving}
           ] = calls
  end

  test "update an indexed field", context do
    tenant = context[:tenant]
    alice = TestRepo.insert!(%User{name: "Alice"}, prefix: tenant)

    {calls, _} =
      with_erlfdb_calls(context.test, fn ->
        changeset = User.changeset(alice, %{name: "Alicia"})
        {:ok, _} = TestRepo.update(changeset)
      end)

    assert [
             # we always get global metadataVersion
             {EctoFoundationDB.Layer.MetadataVersion, :get},
             {EctoFoundationDB.Future, :wait},

             # get and wait for existing data from primary write
             {EctoFoundationDB.Layer.Tx, :get_range},
             {EctoFoundationDB.Future, :wait_for_all_interleaving},

             # set data in primary write
             {EctoFoundationDB.Layer.Tx, :set},

             # clear and set default index. :name has changed
             {EctoFoundationDB.Indexer.Default, :clear},
             {EctoFoundationDB.Indexer.Default, :set}
           ] == calls
  end

  test "update a non-indexed field", context do
    tenant = context[:tenant]
    alice = TestRepo.insert!(%User{name: "Alice"}, prefix: tenant)

    {calls, _} =
      with_erlfdb_calls(context.test, fn ->
        changeset = User.changeset(alice, %{notes: "Hello world"})
        {:ok, _} = TestRepo.update(changeset)
      end)

    assert [
             # we always get global metadataVersion
             {EctoFoundationDB.Layer.MetadataVersion, :get},
             {EctoFoundationDB.Future, :wait},

             # get and wait for existing data from primary write
             {EctoFoundationDB.Layer.Tx, :get_range},
             {EctoFoundationDB.Future, :wait_for_all_interleaving},

             # set data in primary write
             {EctoFoundationDB.Layer.Tx, :set}
           ] == calls
  end

  test "get by indexed field", context do
    tenant = context[:tenant]
    _alice = TestRepo.insert!(%User{name: "Alice"}, prefix: tenant)

    {calls, _} =
      with_erlfdb_calls(context.test, fn ->
        TestRepo.get_by(User, [name: "Alice"], prefix: tenant)
      end)

    assert [
             # we always get global metadataVersion
             {EctoFoundationDB.Layer.MetadataVersion, :get},
             {EctoFoundationDB.Future, :wait},

             # get_mapped_range for :name index. The call to :get_range shown here
             # is a tail call from get_mapped_range, so it's expected and harmless
             {EctoFoundationDB.Layer.Query, :get_mapped_range},
             {EctoFoundationDB.Layer.Query, :get_range},

             # wait for get_mapped_range result
             {EctoFoundationDB.Future, :wait_for_all_interleaving}
           ] == calls
  end

  test "async get by indexed field", context do
    tenant = context[:tenant]
    _alice = TestRepo.insert!(%User{name: "Alice"}, prefix: tenant)
    _bob = TestRepo.insert!(%User{name: "Bob"}, prefix: tenant)

    {calls, _} =
      with_erlfdb_calls(context.test, fn ->
        TestRepo.transactional(
          tenant,
          fn ->
            f1 = TestRepo.async_get_by(User, name: "Alice")
            f2 = TestRepo.async_get_by(User, name: "Bob")
            TestRepo.await([f1, f2])
          end
        )
      end)

    assert [
             # we always get global metadataVersion
             {EctoFoundationDB.Layer.MetadataVersion, :get},
             {EctoFoundationDB.Future, :wait},

             # get_mapped_range for :name index
             {EctoFoundationDB.Layer.Query, :get_mapped_range},
             {EctoFoundationDB.Layer.Query, :get_range},

             # get global again (2nd async_get_by). Not strictly necessary, but there's no cost
             {EctoFoundationDB.Layer.MetadataVersion, :get},
             {EctoFoundationDB.Future, :wait},

             # get_mapped_range for :name index
             {EctoFoundationDB.Layer.Query, :get_mapped_range},
             {EctoFoundationDB.Layer.Query, :get_range},

             # wait for results of both get_mapped_range calls
             {EctoFoundationDB.Future, :wait_for_all_interleaving}
           ] == calls
  end

  test "delete", context do
    tenant = context[:tenant]
    alice = TestRepo.insert!(%User{name: "Alice"}, prefix: tenant)

    {calls, _} =
      with_erlfdb_calls(context.test, fn ->
        {:ok, _} = TestRepo.delete(alice)
      end)

    assert [
             # we always get global metadataVersion
             {EctoFoundationDB.Layer.MetadataVersion, :get},
             {EctoFoundationDB.Future, :wait},

             # check for existence
             {EctoFoundationDB.Layer.Tx, :get_range},
             {EctoFoundationDB.Future, :wait_for_all_interleaving},

             # clear primary
             {EctoFoundationDB.Layer.Tx, :clear},

             # clear :name index
             {EctoFoundationDB.Indexer.Default, :clear}
           ] == calls
  end

  test "insert large object", context do
    tenant = context[:tenant]

    # Prime the cache
    _alice = TestRepo.insert!(%User{name: "Alice"}, prefix: tenant)

    {calls, _eve} =
      with_erlfdb_calls(context.test, fn ->
        {:ok, alice} =
          %User{name: "Eve", notes: Util.get_random_bytes(100_000)}
          |> FoundationDB.usetenant(tenant)
          |> TestRepo.insert()

        alice
      end)

    assert [
             # we always get global metadataVersion
             {EctoFoundationDB.Layer.MetadataVersion, :get},
             {EctoFoundationDB.Future, :wait},

             # check for existence of primary write
             {EctoFoundationDB.Layer.Tx, :get_range},

             # wait for existence check
             {EctoFoundationDB.Future, :wait_for_all_interleaving},

             # primary write
             {EctoFoundationDB.Layer.PrimaryKVCodec, :set},

             # multikey writes
             {EctoFoundationDB.Layer.PrimaryKVCodec, :set},
             {EctoFoundationDB.Layer.PrimaryKVCodec, :set},

             # index write
             {EctoFoundationDB.Indexer.Default, :set}
           ] == calls
  end

  test "update a non-indexed field on a large object into a normal object", context do
    tenant = context[:tenant]

    eve =
      TestRepo.insert!(%User{name: "Eve", notes: Util.get_random_bytes(100_000)}, prefix: tenant)

    {calls, _eve} =
      with_erlfdb_calls(context.test, fn ->
        changeset = User.changeset(eve, %{notes: "Hello world"})
        {:ok, eve} = TestRepo.update(changeset)
        eve
      end)

    assert [
             # we always get global metadataVersion
             {EctoFoundationDB.Layer.MetadataVersion, :get},
             {EctoFoundationDB.Future, :wait},

             # get and wait for existing data from primary write
             {EctoFoundationDB.Layer.Tx, :get_range},
             {EctoFoundationDB.Future, :wait_for_all_interleaving},

             # clear all existing multikey keys
             {EctoFoundationDB.Layer.Tx, :clear_range},

             # set data in primary write
             {EctoFoundationDB.Layer.Tx, :set}
           ] == calls
  end

  test "update a non-indexed field on a normal object into a large object", context do
    tenant = context[:tenant]

    eve =
      TestRepo.insert!(%User{name: "Eve", notes: "Hello world"}, prefix: tenant)

    {calls, _eve} =
      with_erlfdb_calls(context.test, fn ->
        changeset = User.changeset(eve, %{notes: Util.get_random_bytes(100_000)})
        {:ok, eve} = TestRepo.update(changeset)
        eve
      end)

    assert [
             # we always get global metadataVersion
             {EctoFoundationDB.Layer.MetadataVersion, :get},
             {EctoFoundationDB.Future, :wait},

             # get and wait for existing data from primary write
             {EctoFoundationDB.Layer.Tx, :get_range},
             {EctoFoundationDB.Future, :wait_for_all_interleaving},

             # set data in primary write
             {EctoFoundationDB.Layer.Tx, :set},

             # multikey writes
             {EctoFoundationDB.Layer.Tx, :set},
             {EctoFoundationDB.Layer.Tx, :set}
           ] == calls
  end

  test "delete a large object", context do
    tenant = context[:tenant]

    eve =
      TestRepo.insert!(%User{name: "Eve", notes: Util.get_random_bytes(100_000)}, prefix: tenant)

    {calls, _eve} =
      with_erlfdb_calls(context.test, fn ->
        {:ok, _} = TestRepo.delete(eve)
      end)

    assert [
             # we always get global metadataVersion
             {EctoFoundationDB.Layer.MetadataVersion, :get},
             {EctoFoundationDB.Future, :wait},

             # check for existence
             {EctoFoundationDB.Layer.Tx, :get_range},
             {EctoFoundationDB.Future, :wait_for_all_interleaving},

             # clear multikey range
             {EctoFoundationDB.Layer.Tx, :clear_range},

             # clear :name index
             {EctoFoundationDB.Indexer.Default, :clear}
           ] == calls
  end

  test "async_insert_all!", context do
    tenant = context[:tenant]

    # Prime metadata cache
    TestRepo.transactional(tenant, fn tx ->
      TestRepo.insert!(%QueueItem{id: Versionstamp.next(tx)})
    end)

    {calls, _eve} =
      with_erlfdb_calls(context.test, fn ->
        f =
          TestRepo.transactional(tenant, fn ->
            TestRepo.async_insert_all!(QueueItem, [%QueueItem{author: "test", data: "test"}])
          end)

        TestRepo.await(f)
      end)

    assert [
             # we always get global metadataVersion
             {EctoFoundationDB.Layer.MetadataVersion, :get},
             {EctoFoundationDB.Future, :wait},

             # set the primary write and the index
             {EctoFoundationDB.Layer.PrimaryKVCodec, :set_versionstamped_key},
             {EctoFoundationDB.Indexer.Default, :set_versionstamped_key},

             # wait for versionstamp future
             {EctoFoundationDB.Future, :wait_for_all_interleaving}
           ] == calls
  end
end
