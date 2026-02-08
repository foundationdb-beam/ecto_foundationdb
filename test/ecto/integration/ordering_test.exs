alias Ecto.Integration.TestRepo

alias EctoFoundationDB.Exception.Unsupported
alias EctoFoundationDB.Schemas.Event
alias EctoFoundationDB.Schemas.QueueItem

import Ecto.Query

defmodule Ecto.Integration.OrderingTest.Util do
  def put_pk_data(tenant) do
    items = [%QueueItem{data: <<2>>}, %QueueItem{data: <<1>>}, %QueueItem{data: <<3>>}]

    future =
      TestRepo.transactional(
        tenant,
        fn ->
          TestRepo.async_insert_all!(QueueItem, items)
        end
      )

    TestRepo.await(future)
  end

  def put_idx_data(tenant) do
    dates = [~D[2000-01-01], ~D[2001-01-01], ~D[2002-01-01]]
    names = ["Alice", "Bob", "Charlie"]
    times = [~T[00:00:00.000000], ~T[00:00:01.000000], ~T[00:00:02.000000]]

    events =
      for d <- dates, u <- names, t <- times do
        %Event{date: d, user_id: u, time: t}
      end
      |> Enum.shuffle()

    f = TestRepo.transactional(tenant, fn -> TestRepo.async_insert_all!(Event, events) end)

    _ = TestRepo.await(f)
  end
end

# Basic ordering
#
defmodule Ecto.Integration.OrderingTest.OrderByPk do
  use Ecto.Integration.Case, async: true
  import Ecto.Integration.OrderingTest.Util

  test "order by pk", context do
    tenant = context[:tenant]
    [event2, event1, event3] = put_pk_data(tenant)

    assert [^event2, ^event1, ^event3] =
             TestRepo.all(from(q in QueueItem, order_by: {:asc, :id}), prefix: tenant)

    assert [^event3, ^event1, ^event2] =
             TestRepo.all(from(q in QueueItem, order_by: {:desc, :id}), prefix: tenant)
  end
end

defmodule Ecto.Integration.OrderingTest.PkLimit do
  use Ecto.Integration.Case, async: true
  import Ecto.Integration.OrderingTest.Util

  test "pk forward limit", context do
    tenant = context[:tenant]
    [event2, event1, _event3] = put_pk_data(tenant)

    assert [^event2, ^event1] =
             TestRepo.all(from(q in QueueItem, order_by: {:asc, :id}),
               prefix: tenant,
               key_limit: 2
             )
  end

  test "pk backward limit", context do
    tenant = context[:tenant]
    [_event2, event1, event3] = put_pk_data(tenant)

    assert [^event3, ^event1] =
             TestRepo.all(from(q in QueueItem, order_by: {:desc, :id}),
               prefix: tenant,
               key_limit: 2
             )
  end

  test "pk forward query limit", context do
    tenant = context[:tenant]
    [event2, event1, _event3] = put_pk_data(tenant)

    assert [^event2, ^event1] =
             TestRepo.all(from(q in QueueItem, order_by: {:asc, :id}, limit: 2), prefix: tenant)
  end

  test "pk backward query limit", context do
    tenant = context[:tenant]
    [_event2, event1, event3] = put_pk_data(tenant)

    assert [^event3, ^event1] =
             TestRepo.all(from(q in QueueItem, order_by: {:desc, :id}, limit: 2),
               prefix: tenant
             )
  end
end

defmodule Ecto.Integration.OrderingTest.OrderByDataField do
  use Ecto.Integration.Case, async: true
  import Ecto.Integration.OrderingTest.Util

  test "order by data field", context do
    tenant = context[:tenant]
    put_pk_data(tenant)

    # order by non-pk, non-index field is supported when there's no where clause
    assert [_ | _] = TestRepo.all(from(q in QueueItem, order_by: {:asc, :data}), prefix: tenant)
    assert [_ | _] = TestRepo.all(from(q in QueueItem, order_by: {:desc, :data}), prefix: tenant)
  end

  test "data field forward limit", context do
    tenant = context[:tenant]
    [_event2, _event1, _event3] = put_pk_data(tenant)

    # limited ordering not possible, raise error instead of allowing this
    assert_raise Unsupported,
                 ~r/ordering must be/,
                 fn ->
                   TestRepo.all(from(q in QueueItem, order_by: {:asc, :data}),
                     prefix: tenant,
                     key_limit: 2
                   )
                 end
  end

  test "data field backward limit", context do
    tenant = context[:tenant]
    [_event2, _event1, _event3] = put_pk_data(tenant)
    # limited ordering not possible, raise error instead of allowing this
    assert_raise Unsupported, ~r//, fn ->
      TestRepo.all(from(q in QueueItem, order_by: {:desc, :data}), prefix: tenant, key_limit: 2)
    end
  end

  test "data field forward query limit", context do
    tenant = context[:tenant]
    [_event2, _event1, _event3] = put_pk_data(tenant)

    # limited ordering not possible, raise error instead of allowing this
    assert_raise Unsupported,
                 ~r/ordering must be/,
                 fn ->
                   TestRepo.all(from(q in QueueItem, order_by: {:asc, :data}, limit: 2),
                     prefix: tenant
                   )
                 end
  end

  test "data field backward query limit", context do
    tenant = context[:tenant]
    [_event2, _event1, _event3] = put_pk_data(tenant)
    # limited ordering not possible, raise error instead of allowing this
    assert_raise Unsupported, ~r//, fn ->
      TestRepo.all(from(q in QueueItem, order_by: {:desc, :data}, limit: 2), prefix: tenant)
    end
  end
end

## Index ordering

defmodule Ecto.Integration.OrderingTest.IndexWithNoneConstraint do
  use Ecto.Integration.Case, async: true
  import Ecto.Integration.OrderingTest.Util

  test "ordering on index with None constraint", context do
    tenant = context[:tenant]
    put_idx_data(tenant)

    # empty because Event has `primary_write: false`
    assert [] = TestRepo.all(Event, prefix: tenant)

    all_events =
      TestRepo.all(
        from(e in Event, where: e.date > ^~D[0000-01-01] and e.date < ^~D[9999-01-01]),
        prefix: tenant
      )

    assert 27 == length(all_events)

    # index selection is activated by order_by
    assert [%{date: ~D[2000-01-01]} | rest_events] =
             TestRepo.all(from(e in Event, order_by: [asc: e.date]), prefix: tenant)

    assert 26 == length(rest_events)

    assert [%{date: ~D[2002-01-01]} | rest_events] =
             TestRepo.all(from(e in Event, order_by: [desc: e.date]), prefix: tenant)

    assert 26 == length(rest_events)
  end

  test "order by first field in index, with limit", context do
    tenant = context[:tenant]
    put_idx_data(tenant)

    assert [%{date: ~D[2000-01-01], user_id: "Alice", time: ~T[00:00:00.000000]}] =
             TestRepo.all(
               from(e in Event, where: e.date > ^~D[0000-01-01], order_by: [asc: e.date]),
               prefix: tenant,
               key_limit: 1
             )
  end

  test "order by first field in index, with query limit", context do
    tenant = context[:tenant]
    put_idx_data(tenant)

    assert [%{date: ~D[2000-01-01], user_id: "Alice", time: ~T[00:00:00.000000]}] =
             TestRepo.all(
               from(e in Event,
                 where: e.date > ^~D[0000-01-01],
                 order_by: [asc: e.date],
                 limit: 1
               ),
               prefix: tenant
             )
  end

  test "order by first field in index, with backward scan and limit", context do
    tenant = context[:tenant]
    put_idx_data(tenant)

    assert [%{date: ~D[2002-01-01], user_id: "Charlie", time: ~T[00:00:02.000000]}] =
             TestRepo.all(
               from(e in Event, where: e.date > ^~D[0000-01-01], order_by: [desc: e.date]),
               prefix: tenant,
               key_limit: 1
             )
  end

  test "order by first field in index, with backward scan and query limit", context do
    tenant = context[:tenant]
    put_idx_data(tenant)

    assert [%{date: ~D[2002-01-01], user_id: "Charlie", time: ~T[00:00:02.000000]}] =
             TestRepo.all(
               from(e in Event,
                 where: e.date > ^~D[0000-01-01],
                 order_by: [desc: e.date],
                 limit: 1
               ),
               prefix: tenant
             )
  end

  test "order by middle field in index with limit and without Equal constraint on first field",
       context do
    tenant = context[:tenant]
    put_idx_data(tenant)

    assert_raise Unsupported,
                 ~r/ordering must be/,
                 fn ->
                   TestRepo.all(from(e in Event, order_by: [asc: e.user_id]),
                     prefix: tenant,
                     key_limit: 1
                   )
                 end
  end

  test "order by middle field in index with limit and without Equal constraint on first field and query limit",
       context do
    tenant = context[:tenant]
    put_idx_data(tenant)

    assert_raise Unsupported,
                 ~r/ordering must be/,
                 fn ->
                   TestRepo.all(from(e in Event, order_by: [asc: e.user_id], limit: 1),
                     prefix: tenant
                   )
                 end
  end
end

defmodule Ecto.Integration.OrderingTest.IndexWithEqualConstraint do
  use Ecto.Integration.Case, async: true
  import Ecto.Integration.OrderingTest.Util

  test "order by middle field in index with limit and with Equal constraint on first field",
       context do
    tenant = context[:tenant]
    put_idx_data(tenant)

    assert [%{date: ~D[2000-01-01], user_id: "Alice", time: ~T[00:00:00.000000]}] =
             TestRepo.all(
               from(e in Event, where: e.date == ^~D[2000-01-01], order_by: [asc: e.user_id]),
               prefix: tenant,
               key_limit: 1
             )
  end

  test "order by middle field in index with limit and with Equal constraint on first field, backward scan",
       context do
    tenant = context[:tenant]
    put_idx_data(tenant)

    assert [%{date: ~D[2000-01-01], user_id: "Charlie", time: ~T[00:00:02.000000]}] =
             TestRepo.all(
               from(e in Event, where: e.date == ^~D[2000-01-01], order_by: [desc: e.user_id]),
               prefix: tenant,
               key_limit: 1
             )
  end

  test "order by middle field in index with query limit and with Equal constraint on first field",
       context do
    tenant = context[:tenant]
    put_idx_data(tenant)

    assert [%{date: ~D[2000-01-01], user_id: "Alice", time: ~T[00:00:00.000000]}] =
             TestRepo.all(
               from(e in Event,
                 where: e.date == ^~D[2000-01-01],
                 order_by: [asc: e.user_id],
                 limit: 1
               ),
               prefix: tenant
             )
  end

  test "order by middle field in index with query limit and with Equal constraint on first field, backward scan",
       context do
    tenant = context[:tenant]
    put_idx_data(tenant)

    assert [%{date: ~D[2000-01-01], user_id: "Charlie", time: ~T[00:00:02.000000]}] =
             TestRepo.all(
               from(e in Event,
                 where: e.date == ^~D[2000-01-01],
                 order_by: [desc: e.user_id],
                 limit: 1
               ),
               prefix: tenant
             )
  end
end

defmodule Ecto.Integration.OrderingTest.IndexWithBetweenConstraint do
  use Ecto.Integration.Case, async: true
  import Ecto.Integration.OrderingTest.Util

  test "order by index with Between clause", context do
    tenant = context[:tenant]
    put_idx_data(tenant)

    assert [%{date: ~D[2000-01-01], user_id: "Alice", time: ~T[00:00:00.000000]}] =
             TestRepo.all(
               from(e in Event,
                 where:
                   e.date == ^~D[2000-01-01] and e.user_id == ^"Alice" and
                     e.time >= ^~T[00:00:00.000000] and
                     e.time <= ^~T[00:00:01.999999],
                 order_by: [asc: e.time]
               ),
               prefix: tenant,
               key_limit: 1
             )
  end

  test "order by index with Between clause, backward scan", context do
    tenant = context[:tenant]
    put_idx_data(tenant)

    assert [%{date: ~D[2000-01-01], user_id: "Alice", time: ~T[00:00:01.000000]}] =
             TestRepo.all(
               from(e in Event,
                 where:
                   e.date == ^~D[2000-01-01] and e.user_id == ^"Alice" and
                     e.time >= ^~T[00:00:00.000000] and
                     e.time <= ^~T[00:00:01.999999],
                 order_by: [desc: e.time]
               ),
               prefix: tenant,
               key_limit: 1
             )
  end

  test "order by index with Between clause, query limit", context do
    tenant = context[:tenant]
    put_idx_data(tenant)

    assert [%{date: ~D[2000-01-01], user_id: "Alice", time: ~T[00:00:00.000000]}] =
             TestRepo.all(
               from(e in Event,
                 where:
                   e.date == ^~D[2000-01-01] and e.user_id == ^"Alice" and
                     e.time >= ^~T[00:00:00.000000] and
                     e.time <= ^~T[00:00:01.999999],
                 order_by: [asc: e.time],
                 limit: 1
               ),
               prefix: tenant
             )
  end

  test "order by index with Between clause, backward scan, query limit", context do
    tenant = context[:tenant]
    put_idx_data(tenant)

    assert [%{date: ~D[2000-01-01], user_id: "Alice", time: ~T[00:00:01.000000]}] =
             TestRepo.all(
               from(e in Event,
                 where:
                   e.date == ^~D[2000-01-01] and e.user_id == ^"Alice" and
                     e.time >= ^~T[00:00:00.000000] and
                     e.time <= ^~T[00:00:01.999999],
                 order_by: [desc: e.time],
                 limit: 1
               ),
               prefix: tenant
             )
  end
end

defmodule Ecto.Integration.OrderingTest.QueryLimitWithSplitObjects do
  use Ecto.Integration.Case, async: true

  alias EctoFoundationDB.Schemas.User
  alias EctoFoundationDB.Test.Util

  import Ecto.Query

  test "option limit", context do
    tenant = context[:tenant]

    assert {:ok, _alice} =
             %User{id: "alice", name: "Alice", notes: Util.get_random_bytes(100)}
             |> TestRepo.insert(max_single_value_size: 100_000, prefix: tenant)

    assert {:ok, _bob} =
             %User{id: "bob", name: "Bob", notes: Util.get_random_bytes(100_000)}
             |> TestRepo.insert(max_single_value_size: 100_000, prefix: tenant)

    assert [%{id: "alice"}] = TestRepo.all(User, prefix: tenant, key_limit: 1)
    assert [%{id: "alice"}] = TestRepo.all(User, prefix: tenant, key_limit: 2)
  end

  test "query limit", context do
    tenant = context[:tenant]

    assert {:ok, _alice} =
             %User{id: "alice", name: "Alice", notes: Util.get_random_bytes(100)}
             |> TestRepo.insert(max_single_value_size: 100_000, prefix: tenant)

    assert {:ok, _bob} =
             %User{id: "bob", name: "Bob", notes: Util.get_random_bytes(100_000)}
             |> TestRepo.insert(max_single_value_size: 100_000, prefix: tenant)

    assert [%{id: "alice"}] = TestRepo.all(from(u in User, limit: 1), prefix: tenant)

    assert [%{id: "alice"}, %{id: "bob"}] =
             TestRepo.all(from(u in User, limit: 2), prefix: tenant)

    assert [%{id: "alice"}] =
             TestRepo.all(from(u in User, limit: 2), prefix: tenant, key_limit: 3)

    assert [%{id: "alice"}, %{id: "bob"}] =
             TestRepo.all(from(u in User, limit: 2), prefix: tenant, key_limit: 4)
  end
end

defmodule Ecto.Integration.OrderingTest.MultipleOrderBys do
  use Ecto.Integration.Case, async: true

  alias EctoFoundationDB.Schemas.User
  alias EctoFoundationDB.Test.Util

  import Ecto.Query

  test "option limit", context do
    tenant = context[:tenant]

    users = [
      %User{id: "1", name: "Alice", notes: "foo", inserted_at: ~N[2026-02-01 00:00:01]},
      %User{id: "2", name: "Bob", notes: "foo", inserted_at: ~N[2026-02-01 00:00:02]},
      %User{id: "3", name: "Alice", notes: "bar", inserted_at: ~N[2026-02-01 00:00:03]},
      %User{id: "4", name: "Alice", notes: "foo", inserted_at: ~N[2026-02-01 00:00:04]},
      %User{id: "5", name: "Bob", notes: "foo", inserted_at: ~N[2026-02-01 00:00:05]},
      %User{id: "6", name: "Alice", notes: "bar", inserted_at: ~N[2026-02-01 00:00:06]}
    ]

    for u <- users, do: TestRepo.insert(u, prefix: tenant)

    query =
      from(u in User, order_by: [asc: u.name, desc: u.inserted_at])

    assert [%{id: "6"}, %{id: "4"}, %{id: "3"}, %{id: "1"}, %{id: "5"}, %{id: "2"}] =
             TestRepo.all(query, prefix: tenant)
  end
end
