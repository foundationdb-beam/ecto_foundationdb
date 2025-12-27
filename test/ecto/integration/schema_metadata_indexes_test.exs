defmodule EctoFoundationDBSchemaMetadataIndexesTest do
  use Ecto.Integration.Case, async: true

  alias Ecto.Integration.TestRepo
  alias EctoFoundationDB.Indexer.SchemaMetadata
  alias EctoFoundationDB.Schemas.User

  defp idcuc_by(tenant, schema, by) do
    TestRepo.transactional(tenant, fn ->
      [
        SchemaMetadata.async_inserts_by(schema, by),
        SchemaMetadata.async_deletes_by(schema, by),
        SchemaMetadata.async_collection_by(schema, by),
        SchemaMetadata.async_updates_by(schema, by),
        SchemaMetadata.async_changes_by(schema, by)
      ]
      |> TestRepo.await()
    end)
  end

  test "counters", context do
    tenant = context[:tenant]
    assert [0, 0, 0, 0, 0] = idcuc_by(tenant, User, name: "Alice")

    user = %User{} = TestRepo.insert!(%User{name: "Alice"}, prefix: tenant)

    # insert, collection, changes
    assert [1, 0, 1, 0, 1] = idcuc_by(tenant, User, name: "Alice")

    TestRepo.insert!(%User{name: "Bob"}, prefix: tenant)

    # no-op on name: "Alive", so no change to counters
    assert [1, 0, 1, 0, 1] = idcuc_by(tenant, User, name: "Alice")

    {:ok, user} =
      TestRepo.update(User.changeset(user, %{notes: "A change to a non-indexed field"}),
        prefix: tenant
      )

    # update, changes
    assert [1, 0, 1, 1, 2] = idcuc_by(tenant, User, name: "Alice")

    {:ok, user} = TestRepo.update(User.changeset(user, %{name: "Alicia"}), prefix: tenant)

    # delete, collection, update, changes
    assert [1, 1, 2, 2, 3] = idcuc_by(tenant, User, name: "Alice")

    {:ok, user} = TestRepo.update(User.changeset(user, %{name: "Alice"}), prefix: tenant)

    # insert, collection, update, changes
    assert [2, 1, 3, 3, 4] = idcuc_by(tenant, User, name: "Alice")

    TestRepo.delete!(user, prefix: tenant)

    # delete, collection, changes
    assert [2, 2, 4, 3, 5] = idcuc_by(tenant, User, name: "Alice")

    future =
      TestRepo.transactional(tenant, fn ->
        SchemaMetadata.watch_collection_by(User, name: "Alice")
      end)

    TestRepo.transactional(tenant, fn ->
      TestRepo.insert!(%User{name: "Alice", notes: "another one"})
    end)

    assert _ = TestRepo.await(future)

    TestRepo.transactional(tenant, fn -> SchemaMetadata.clear_by(User, name: "Alice") end)

    assert [0, 0, 0, 0, 0] = idcuc_by(tenant, User, name: "Alice")
  end
end
