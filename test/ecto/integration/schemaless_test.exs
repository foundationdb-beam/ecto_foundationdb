defmodule Ecto.Integration.SchemalessTest do
  use Ecto.Integration.Case, async: true

  import Ecto.Query

  alias EctoFoundationDB.Schemas.QueueItem
  alias EctoFoundationDB.Schemas.Session
  alias EctoFoundationDB.Schemas.User
  alias EctoFoundationDB.Versionstamp
  alias Ecto.Integration.TestRepo

  @moduletag :integration

  describe "schemaless full scan (users)" do
    test "returns all records", context do
      tenant = context[:tenant]

      TestRepo.transactional(tenant, fn ->
        for name <- ~w/Alice Bob Charlie/ do
          TestRepo.insert(%User{name: name})
        end
      end)

      results =
        from(u in "users", select: map(u, [:name]))
        |> TestRepo.all(prefix: tenant)

      assert length(results) == 3
      assert Enum.all?(results, &is_map/1)
      assert ["Alice", "Bob", "Charlie"] == Enum.map(results, & &1.name) |> Enum.sort()
    end

    test "select list expression returns value lists", context do
      tenant = context[:tenant]

      TestRepo.transactional(tenant, fn ->
        for name <- ~w/Alice Bob/ do
          TestRepo.insert(%User{name: name})
        end
      end)

      results =
        from(u in "users", select: [u.name])
        |> TestRepo.all(prefix: tenant)
        |> Enum.sort()

      assert [["Alice"], ["Bob"]] = results
    end

    test "limit restricts result count", context do
      tenant = context[:tenant]

      TestRepo.transactional(tenant, fn ->
        for name <- ~w/Alice Bob Charlie Dave/ do
          TestRepo.insert(%User{name: name})
        end
      end)

      results =
        from(u in "users", select: map(u, [:name]), limit: 2)
        |> TestRepo.all(prefix: tenant)

      assert length(results) == 2
    end
  end

  describe "schemaless PK range queries (users)" do
    test "between two PKs returns matching records", context do
      tenant = context[:tenant]

      TestRepo.insert(%User{id: "0001", name: "Alice"}, prefix: tenant)
      TestRepo.insert(%User{id: "0002", name: "Bob"}, prefix: tenant)
      TestRepo.insert(%User{id: "0003", name: "Charlie"}, prefix: tenant)

      results =
        from(u in "users",
          where: u.id >= ^"0001" and u.id <= ^"0002",
          select: map(u, [:id, :name])
        )
        |> TestRepo.all(prefix: tenant)

      assert length(results) == 2
      names = Enum.map(results, & &1.name) |> Enum.sort()
      assert names == ["Alice", "Bob"]
    end

    test "all_range with string source needs no select clause", context do
      tenant = context[:tenant]

      TestRepo.insert(%User{id: "0001", name: "Alice"}, prefix: tenant)
      TestRepo.insert(%User{id: "0002", name: "Bob"}, prefix: tenant)
      TestRepo.insert(%User{id: "0003", name: "Charlie"}, prefix: tenant)

      results = TestRepo.all_range("users", "0001", "0002", inclusive_right?: true, prefix: tenant)

      assert length(results) == 2
      assert Enum.all?(results, &is_map/1)
      names = Enum.map(results, & &1.name) |> Enum.sort()
      assert names == ["Alice", "Bob"]
    end

    test "all_range with from query and no select returns all fields as maps", context do
      tenant = context[:tenant]

      TestRepo.insert(%User{id: "0001", name: "Alice"}, prefix: tenant)
      TestRepo.insert(%User{id: "0002", name: "Bob"}, prefix: tenant)
      TestRepo.insert(%User{id: "0003", name: "Charlie"}, prefix: tenant)

      results =
        TestRepo.all_range(from("users"), "0001", "0002", inclusive_right?: true, prefix: tenant)

      assert length(results) == 2
      assert Enum.all?(results, &is_map/1)
      names = Enum.map(results, & &1.name) |> Enum.sort()
      assert names == ["Alice", "Bob"]
    end

    test "all_range with from query and select returns reduced fields", context do
      tenant = context[:tenant]

      TestRepo.insert(%User{id: "0001", name: "Alice"}, prefix: tenant)
      TestRepo.insert(%User{id: "0002", name: "Bob"}, prefix: tenant)

      results =
        TestRepo.all_range(from("users", select: [:id, :name]), "0001", "0002",
          inclusive_right?: true,
          prefix: tenant
        )

      assert length(results) == 2
      assert Enum.all?(results, fn r -> Map.keys(r) |> Enum.sort() == [:id, :name] end)
    end
  end

  describe "schemaless index queries (users)" do
    test "equality on indexed field returns matching records", context do
      tenant = context[:tenant]

      TestRepo.transactional(tenant, fn ->
        TestRepo.insert(%User{name: "Alice"})
        TestRepo.insert(%User{name: "Bob"})
        TestRepo.insert(%User{name: "Bob"})
      end)

      results =
        from(u in "users", where: u.name == ^"Bob", select: map(u, [:name]))
        |> TestRepo.all(prefix: tenant)

      assert length(results) == 2
      assert Enum.all?(results, fn r -> r.name == "Bob" end)
    end

    test "equality returns empty list when no match", context do
      tenant = context[:tenant]

      TestRepo.transactional(tenant, fn ->
        TestRepo.insert(%User{name: "Alice"})
      end)

      assert [] =
               from(u in "users", where: u.name == ^"Zara", select: map(u, [:name]))
               |> TestRepo.all(prefix: tenant)
    end

    test "between range on indexed string field", context do
      tenant = context[:tenant]

      TestRepo.transactional(tenant, fn ->
        for name <- ~w/Alice Bob Charlie Dave/ do
          TestRepo.insert(%User{name: name})
        end
      end)

      results =
        from(u in "users",
          where: u.name >= ^"Bob" and u.name <= ^"Charlie",
          select: map(u, [:name])
        )
        |> TestRepo.all(prefix: tenant)

      names = Enum.map(results, & &1.name) |> Enum.sort()
      assert names == ["Bob", "Charlie"]
    end
  end

  describe "schemaless queries on non-partitioned versionstamp schema (queue)" do
    test "full scan returns all items", context do
      tenant = context[:tenant]

      future =
        TestRepo.transactional(tenant, fn ->
          TestRepo.async_insert_all!(QueueItem, [
            %QueueItem{author: "alice", data: "a1"},
            %QueueItem{author: "bob", data: "b1"},
            %QueueItem{author: "charlie", data: "c1"}
          ])
        end)

      TestRepo.await(future)

      results =
        from(q in "queue", select: map(q, [:author]))
        |> TestRepo.all(prefix: tenant)

      assert length(results) == 3
      authors = Enum.map(results, & &1.author) |> Enum.sort()
      assert authors == ["alice", "bob", "charlie"]
    end

    test "index equality on author field returns matching items", context do
      tenant = context[:tenant]

      future =
        TestRepo.transactional(tenant, fn ->
          TestRepo.async_insert_all!(QueueItem, [
            %QueueItem{author: "alice", data: "a1"},
            %QueueItem{author: "bob", data: "b1"},
            %QueueItem{author: "alice", data: "a2"}
          ])
        end)

      TestRepo.await(future)

      results =
        from(q in "queue", where: q.author == ^"alice", select: map(q, [:author, :data]))
        |> TestRepo.all(prefix: tenant)

      assert length(results) == 2
      assert Enum.all?(results, fn r -> r.author == "alice" end)
    end

    test "PK range query scopes to items between two versionstamps", context do
      tenant = context[:tenant]

      future =
        TestRepo.transactional(tenant, fn ->
          TestRepo.async_insert_all!(QueueItem, [
            %QueueItem{author: "alice", data: "a1"},
            %QueueItem{author: "bob", data: "b1"},
            %QueueItem{author: "charlie", data: "c1"}
          ])
        end)

      [item1, item2, _item3] = TestRepo.await(future)

      results =
        from(q in "queue",
          where: q.id >= ^item1.id and q.id <= ^item2.id,
          select: map(q, [:author])
        )
        |> TestRepo.all(prefix: tenant)

      assert length(results) == 2
      authors = Enum.map(results, & &1.author) |> Enum.sort()
      assert authors == ["alice", "bob"]
    end

    test "select list expression on filtered results", context do
      tenant = context[:tenant]

      future =
        TestRepo.transactional(tenant, fn ->
          TestRepo.async_insert_all!(QueueItem, [
            %QueueItem{author: "alice", data: "x"}
          ])
        end)

      TestRepo.await(future)

      results =
        from(q in "queue", where: q.author == ^"alice", select: [q.author])
        |> TestRepo.all(prefix: tenant)

      assert [["alice"]] = results
    end
  end

  describe "schemaless queries on partitioned versionstamp schema (sessions)" do
    setup context do
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

      {:ok, alice1: alice1, alice2: alice2, bob1: bob1}
    end

    test "full scan returns all sessions across partitions", context do
      tenant = context[:tenant]

      results =
        from(s in "sessions", select: map(s, [:user_id, :data]))
        |> TestRepo.all(prefix: tenant)

      assert length(results) == 3
      assert Enum.all?(results, &is_map/1)
      users = Enum.map(results, & &1.user_id) |> Enum.sort()
      assert users == ["alice", "alice", "bob"]
    end

    test "partition scan using min/max returns only that user's sessions", context do
      tenant = context[:tenant]

      results =
        from(s in "sessions",
          where:
            s.id >= ^{"alice", Versionstamp.min()} and s.id <= ^{"alice", Versionstamp.max()},
          select: map(s, [:user_id, :data])
        )
        |> TestRepo.all(prefix: tenant)

      assert length(results) == 2
      assert Enum.all?(results, fn r -> r.user_id == "alice" end)
    end

    test "compound equality gets exact session in partition", context do
      tenant = context[:tenant]
      alice1 = context[:alice1]

      results =
        from(s in "sessions",
          where: s.id == ^{"alice", alice1.id},
          select: map(s, [:user_id, :data])
        )
        |> TestRepo.all(prefix: tenant)

      assert [%{data: "a1", user_id: "alice"}] = results
    end

    test "between checkpoints stays within partition", context do
      tenant = context[:tenant]
      alice1 = context[:alice1]
      alice2 = context[:alice2]

      results =
        from(s in "sessions",
          where: s.id >= ^{"alice", alice1.id} and s.id <= ^{"alice", alice2.id},
          select: map(s, [:user_id, :data])
        )
        |> TestRepo.all(prefix: tenant)

      assert length(results) == 2
      assert Enum.all?(results, fn r -> r.user_id == "alice" end)
    end

    test "partition scan does not bleed into other users' data", context do
      tenant = context[:tenant]

      alice_results =
        from(s in "sessions",
          where:
            s.id >= ^{"alice", Versionstamp.min()} and s.id <= ^{"alice", Versionstamp.max()},
          select: map(s, [:user_id])
        )
        |> TestRepo.all(prefix: tenant)

      bob_results =
        from(s in "sessions",
          where:
            s.id >= ^{"bob", Versionstamp.min()} and s.id <= ^{"bob", Versionstamp.max()},
          select: map(s, [:user_id])
        )
        |> TestRepo.all(prefix: tenant)

      assert length(alice_results) == 2
      assert length(bob_results) == 1
      assert Enum.all?(alice_results, fn r -> r.user_id == "alice" end)
      assert Enum.all?(bob_results, fn r -> r.user_id == "bob" end)
    end

    test "select list expression on partition scan", context do
      tenant = context[:tenant]

      results =
        from(s in "sessions",
          where:
            s.id >= ^{"alice", Versionstamp.min()} and s.id <= ^{"alice", Versionstamp.max()},
          select: [s.user_id, s.data]
        )
        |> TestRepo.all(prefix: tenant)
        |> Enum.sort()

      assert [["alice", "a1"], ["alice", "a2"]] = results
    end

    test "limit on partition scan", context do
      tenant = context[:tenant]

      results =
        from(s in "sessions",
          where:
            s.id >= ^{"alice", Versionstamp.min()} and s.id <= ^{"alice", Versionstamp.max()},
          select: map(s, [:user_id]),
          limit: 1
        )
        |> TestRepo.all(prefix: tenant)

      assert length(results) == 1
      assert hd(results).user_id == "alice"
    end
  end
end
