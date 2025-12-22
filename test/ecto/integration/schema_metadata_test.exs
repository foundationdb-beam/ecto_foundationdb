defmodule EctoFoundationDBSchemaMetadataTest do
  use Ecto.Integration.Case, async: true

  alias Ecto.Integration.TestRepo
  alias EctoFoundationDB.Future
  alias EctoFoundationDB.Indexer.SchemaMetadata
  alias EctoFoundationDB.Schemas.Event
  alias EctoFoundationDB.Schemas.User
  alias EctoFoundationDB.Tenant

  defp idcuc(tenant, schema) do
    TestRepo.transactional(tenant, fn ->
      [
        SchemaMetadata.async_inserts(schema),
        SchemaMetadata.async_deletes(schema),
        SchemaMetadata.async_collection(schema),
        SchemaMetadata.async_updates(schema),
        SchemaMetadata.async_changes(schema)
      ]
      |> TestRepo.await()
    end)
  end

  test "counters", context do
    tenant = context[:tenant]
    assert [0, 0, 0, 0, 0] = idcuc(tenant, User)

    user = %User{} = TestRepo.insert!(%User{name: "Alice"}, prefix: tenant)

    assert [1, 0, 1, 0, 1] = idcuc(tenant, User)

    {:ok, _} = TestRepo.update(User.changeset(user, %{name: "Bob"}), prefix: tenant)

    assert [1, 0, 1, 1, 2] = idcuc(tenant, User)

    TestRepo.delete!(user, prefix: tenant)

    assert [1, 1, 2, 1, 3] = idcuc(tenant, User)

    future =
      TestRepo.transactional(tenant, fn ->
        SchemaMetadata.watch_collection(User)
      end)

    TestRepo.transactional(tenant, fn ->
      user = TestRepo.insert!(%User{name: "Alice2"})
      TestRepo.delete!(user)
    end)

    assert _ = TestRepo.await(future)
  end

  test "labeled watch", context do
    tenant = context[:tenant]

    # init
    {users, futures} =
      TestRepo.transactional(tenant, fn ->
        users = TestRepo.all(User)
        future = SchemaMetadata.watch_collection(User, label: :users)
        {users, [future]}
      end)

    assigns = %{users: users}

    assert [] = users

    [watch_future] = futures
    watch_ref = Future.ref(watch_future)

    # insert and receive the new collection
    TestRepo.insert!(%User{name: "Alice"}, prefix: tenant)

    {assigns, futures} =
      receive_ready(assigns, futures, [watch_ref], prefix: tenant, watch?: true)

    assert [_] = futures
    refute watch_future == hd(futures)

    [watch_future] = futures
    watch_ref = Future.ref(watch_future)

    assert %{users: [user = %{name: "Alice"}]} = assigns
    assert %Tenant{} = hd(assigns.users).__meta__.prefix

    # delete and receive the new collection
    TestRepo.delete!(user, prefix: tenant)

    {assigns, futures} =
      receive_ready(assigns, futures, [watch_ref], prefix: tenant, watch?: true)

    assert [_] = futures

    assert %{users: []} = assigns
  end

  test "labeled query watch", context do
    tenant = context[:tenant]

    import Ecto.Query

    query = from(e in Event, where: e.date >= ^~D[2025-07-15])

    insert_event = fn date ->
      f =
        TestRepo.transactional(tenant, fn ->
          TestRepo.async_insert_all!(Event, [
            %Event{date: date, user_id: "foo", time: ~T[00:00:00.000000]}
          ])
        end)

      [_] = TestRepo.await(f)
    end

    insert_event.(~D[2025-07-14])

    # init
    {events, futures} =
      TestRepo.transactional(tenant, fn ->
        events = TestRepo.all(query)
        future = SchemaMetadata.watch_collection(Event, label: :events, query: query)
        {events, [future]}
      end)

    assigns = %{events: events}

    assert [] = events

    [watch_future] = futures
    watch_ref = Future.ref(watch_future)

    # insert and receive the new collection
    insert_event.(~D[2025-07-16])

    {assigns, futures} =
      receive_ready(assigns, futures, [watch_ref], prefix: tenant, watch?: false)

    assert [] = futures

    assert %{events: [%{user_id: "foo"}]} = assigns
  end

  defp receive_ready(assigns, futures, [watch_ref], opts) do
    receive do
      {^watch_ref, :ready} when is_reference(watch_ref) ->
        {ready_assigns, new_futures, other_futures} =
          TestRepo.assign_ready(futures, [watch_ref], opts)

        assert [_] = ready_assigns
        assert is_list(ready_assigns)

        {Map.merge(assigns, Enum.into(ready_assigns, %{})), new_futures ++ other_futures}
    after
      100 ->
        raise "Future result not received within 100 msec"
    end
  end
end
