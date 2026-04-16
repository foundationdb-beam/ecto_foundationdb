defmodule Ecto.Integration.PartitionTest do
  use Ecto.Integration.Case, async: true

  import Ecto.Query

  alias Ecto.Adapters.FoundationDB

  alias EctoFoundationDB.Exception.Unsupported
  alias EctoFoundationDB.Schemas.Session
  alias EctoFoundationDB.Versionstamp

  alias Ecto.Integration.TestRepo

  describe "partitioned versionstamp primary key" do
    test "insert stores records in partition-scoped keyspace", context do
      tenant = context[:tenant]

      future =
        TestRepo.transactional(tenant, fn ->
          TestRepo.async_insert_all!(Session, [
            %Session{user_id: "alice", data: "a1"},
            %Session{user_id: "alice", data: "a2"},
            %Session{user_id: "bob", data: "b1"}
          ])
        end)

      [alice1, alice2, bob1] = TestRepo.await(future)

      assert alice1.user_id == "alice"
      assert alice2.user_id == "alice"
      assert bob1.user_id == "bob"

      # All three are retrievable
      all = TestRepo.all(Session, prefix: tenant)
      assert length(all) == 3
    end

    test "single-sided inequality raises Unsupported on partitioned schemas", context do
      tenant = context[:tenant]

      future =
        TestRepo.transactional(tenant, fn ->
          TestRepo.async_insert_all!(Session, [
            %Session{user_id: "alice", data: "a1"},
            %Session{user_id: "alice", data: "a2"}
          ])
        end)

      [alice1, _alice2] = TestRepo.await(future)

      assert_raise Unsupported, ~r/Single-sided range queries are not supported/, fn ->
        TestRepo.all(from(s in Session, where: s.id >= ^{"alice", alice1.id}), prefix: tenant)
      end

      assert_raise Unsupported, ~r/Single-sided range queries are not supported/, fn ->
        TestRepo.all(from(s in Session, where: s.id <= ^{"alice", alice1.id}), prefix: tenant)
      end
    end

    test "Versionstamp.min() and max() together scan an entire partition", context do
      tenant = context[:tenant]

      future =
        TestRepo.transactional(tenant, fn ->
          TestRepo.async_insert_all!(Session, [
            %Session{user_id: "alice", data: "a1"},
            %Session{user_id: "alice", data: "a2"},
            %Session{user_id: "bob", data: "b1"}
          ])
        end)

      [alice1, alice2, _bob1] = TestRepo.await(future)

      # min() as the lower bound and max() as the upper bound together cover
      # all of alice's records without crossing into bob's partition.
      results =
        TestRepo.all(
          from(s in Session,
            where:
              s.id >= ^{"alice", Versionstamp.min()} and s.id <= ^{"alice", Versionstamp.max()}
          ),
          prefix: tenant
        )

      assert results == [alice1, alice2]
      assert Enum.all?(results, fn s -> s.user_id == "alice" end)
    end

    test "Versionstamp.max() as upper bound scopes query from a checkpoint to end of partition",
         context do
      tenant = context[:tenant]

      future =
        TestRepo.transactional(tenant, fn ->
          TestRepo.async_insert_all!(Session, [
            %Session{user_id: "alice", data: "a1"},
            %Session{user_id: "alice", data: "a2"},
            %Session{user_id: "bob", data: "b1"}
          ])
        end)

      [alice1, alice2, _bob1] = TestRepo.await(future)

      # Using max() keeps the scan within alice's partition even though
      # {"bob", _} > {"alice", _} would otherwise be included by an open-ended >=.
      results =
        TestRepo.all(
          from(s in Session,
            where: s.id >= ^{"alice", alice1.id} and s.id <= ^{"alice", Versionstamp.max()}
          ),
          prefix: tenant
        )

      assert results == [alice1, alice2]
      assert Enum.all?(results, fn s -> s.user_id == "alice" end)
    end

    test "range query with both bounds stays within partition", context do
      tenant = context[:tenant]

      future =
        TestRepo.transactional(tenant, fn ->
          TestRepo.async_insert_all!(Session, [
            %Session{user_id: "alice", data: "a1"},
            %Session{user_id: "alice", data: "a2"},
            %Session{user_id: "alice", data: "a3"}
          ])
        end)

      [a1, a2, _a3] = TestRepo.await(future)

      # Range query between two checkpoints for alice
      results =
        TestRepo.all(
          from(s in Session, where: s.id >= ^{"alice", a1.id} and s.id <= ^{"alice", a2.id}),
          prefix: tenant
        )

      assert length(results) == 2
      assert Enum.all?(results, fn s -> s.user_id == "alice" end)
    end

    test "equality query with partition key gets exact record", context do
      tenant = context[:tenant]

      future =
        TestRepo.transactional(tenant, fn ->
          TestRepo.async_insert_all!(Session, [
            %Session{user_id: "alice", data: "target"},
            %Session{user_id: "bob", data: "other"}
          ])
        end)

      [alice, _bob] = TestRepo.await(future)

      # Get exact session using compound {partition, id} param
      results =
        TestRepo.all(
          from(s in Session, where: s.id == ^{"alice", alice.id}),
          prefix: tenant
        )

      assert [%Session{data: "target"}] = results
    end

    test "Repo.get with compound pk returns the record", context do
      tenant = context[:tenant]

      future =
        TestRepo.transactional(tenant, fn ->
          TestRepo.async_insert_all!(Session, [
            %Session{user_id: "alice", data: "target"},
            %Session{user_id: "bob", data: "other"}
          ])
        end)

      [alice, _bob] = TestRepo.await(future)

      # Compound {partition_value, id} locates the record in the correct keyspace.
      assert %Session{data: "target"} =
               TestRepo.get(Session, {"alice", alice.id}, prefix: tenant)

      # A bare versionstamp without a partition value raises because the adapter
      # cannot determine which partition keyspace to search.
      assert_raise Unsupported, ~r/compound \{partition_value, versionstamp\} parameter/, fn ->
        TestRepo.get(Session, alice.id, prefix: tenant)
      end
    end

    test "delete_all with compound pk removes record", context do
      tenant = context[:tenant]

      future =
        TestRepo.transactional(tenant, fn ->
          TestRepo.async_insert_all!(Session, [
            %Session{user_id: "alice", data: "to-delete"},
            %Session{user_id: "alice", data: "to-keep"}
          ])
        end)

      [to_delete, _to_keep] = TestRepo.await(future)

      TestRepo.delete_all(
        from(s in Session, where: s.id == ^{to_delete.user_id, to_delete.id}),
        prefix: tenant
      )

      remaining = TestRepo.all(Session, prefix: tenant)

      assert Enum.all?(remaining, fn s -> s.data != "to-delete" end)
    end

    test "Repo.delete raises Unsupported for partitioned schemas", context do
      tenant = context[:tenant]

      future =
        TestRepo.transactional(tenant, fn ->
          TestRepo.async_insert_all!(Session, [
            %Session{user_id: "alice", data: "data"}
          ])
        end)

      [session] = TestRepo.await(future)

      assert_raise Unsupported, ~r/Repo.delete is not supported/, fn ->
        TestRepo.delete!(session |> FoundationDB.usetenant(tenant))
      end
    end

    test "update_all with compound pk changes non-partition fields", context do
      tenant = context[:tenant]

      future =
        TestRepo.transactional(tenant, fn ->
          TestRepo.async_insert_all!(Session, [
            %Session{user_id: "alice", data: "original"}
          ])
        end)

      [session] = TestRepo.await(future)

      # Update a non-partition field using a compound pk constraint
      TestRepo.update_all(
        from(s in Session, where: s.id == ^{session.user_id, session.id}),
        [set: [data: "updated"]],
        prefix: tenant
      )

      # Verify we can still query it
      results =
        TestRepo.all(
          from(s in Session, where: s.id == ^{"alice", session.id}),
          prefix: tenant
        )

      assert [%Session{data: "updated"}] = results
    end

    test "Repo.update raises Unsupported for partitioned schemas", context do
      tenant = context[:tenant]

      future =
        TestRepo.transactional(tenant, fn ->
          TestRepo.async_insert_all!(Session, [
            %Session{user_id: "alice", data: "data"}
          ])
        end)

      [session] = TestRepo.await(future)

      assert_raise Unsupported, ~r/Repo.update is not supported/, fn ->
        session
        |> FoundationDB.usetenant(tenant)
        |> Ecto.Changeset.change(data: "updated")
        |> TestRepo.update()
      end
    end

    test "update_all raises when changing the partition field", context do
      tenant = context[:tenant]

      future =
        TestRepo.transactional(tenant, fn ->
          TestRepo.async_insert_all!(Session, [
            %Session{user_id: "alice", data: "data"}
          ])
        end)

      [session] = TestRepo.await(future)

      assert_raise Unsupported, ~r/Cannot change the partition_by field/, fn ->
        TestRepo.update_all(
          from(s in Session, where: s.id == ^{session.user_id, session.id}),
          [set: [user_id: "bob"]],
          prefix: tenant
        )
      end
    end

    test "multi-user insert + per-user scoped delete_all", context do
      tenant = context[:tenant]

      future =
        TestRepo.transactional(tenant, fn ->
          TestRepo.async_insert_all!(Session, [
            %Session{user_id: "alice", data: "a1"},
            %Session{user_id: "alice", data: "a2"},
            %Session{user_id: "bob", data: "b1"}
          ])
        end)

      [_alice1, _alice2, _bob1] = TestRepo.await(future)

      # Delete all of alice's sessions using a partition-scoped range
      TestRepo.delete_all(
        from(s in Session,
          where: s.id >= ^{"alice", Versionstamp.min()} and s.id <= ^{"alice", Versionstamp.max()}
        ),
        prefix: tenant
      )

      remaining = TestRepo.all(Session, prefix: tenant)
      assert Enum.all?(remaining, fn s -> s.user_id == "bob" end)
    end

    test "full scan returns records across all partitions", context do
      tenant = context[:tenant]

      future =
        TestRepo.transactional(tenant, fn ->
          TestRepo.async_insert_all!(Session, [
            %Session{user_id: "alice", data: "a1"},
            %Session{user_id: "bob", data: "b1"},
            %Session{user_id: "charlie", data: "c1"}
          ])
        end)

      TestRepo.await(future)

      all = TestRepo.all(Session, prefix: tenant)
      assert length(all) == 3

      users = Enum.map(all, & &1.user_id) |> Enum.sort()
      assert users == ["alice", "bob", "charlie"]
    end
  end
end
