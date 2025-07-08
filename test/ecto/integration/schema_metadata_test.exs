defmodule EctoFoundationDBSchemaMetadataTest do
  use Ecto.Integration.Case, async: true

  alias Ecto.Integration.TestRepo
  alias EctoFoundationDB.Future
  alias EctoFoundationDB.Indexer.SchemaMetadata
  alias EctoFoundationDB.Schemas.User

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

    # delete and receive the new collection
    TestRepo.delete!(user, prefix: tenant)

    {assigns, futures} =
      receive_ready(assigns, futures, [watch_ref], prefix: tenant, watch?: true)

    assert [_] = futures

    assert %{users: []} = assigns
  end

  defp receive_ready(assigns, futures, [watch_ref], opts) do
    receive do
      {^watch_ref, :ready} when is_reference(watch_ref) ->
        {ready_assigns, futures} =
          TestRepo.assign_ready(futures, [watch_ref], opts)

        assert [_] = ready_assigns
        assert is_list(ready_assigns)

        {Map.merge(assigns, Enum.into(ready_assigns, %{})), futures}
    after
      100 ->
        raise "Future result not received within 100 msec"
    end
  end
end
