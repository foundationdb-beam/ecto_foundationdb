defmodule EctoIntegrationUpsertTest do
  use Ecto.Integration.Case, async: true

  alias Ecto.Integration.TestRepo
  alias EctoFoundationDB.Exception.Unsupported
  alias EctoFoundationDB.Schemas.User
  import Ecto.Query

  test "on_conflict: :raise", context do
    tenant = context[:tenant]

    user = TestRepo.insert!(%User{name: "John"}, prefix: tenant)

    assert_raise(Unsupported, ~r/Key exists/, fn ->
      TestRepo.insert!(%User{user | name: "NotJohn"}, prefix: tenant, on_conflict: :raise)
    end)
  end

  test "on_conflict: :nothing", context do
    tenant = context[:tenant]

    user = TestRepo.insert!(%User{name: "John"}, prefix: tenant)

    TestRepo.insert!(%User{user | name: "NotJohn"}, prefix: tenant, on_conflict: :nothing)

    assert %User{name: "John"} = TestRepo.get(User, user.id, prefix: tenant)
  end

  test "on_conflict: :replace_all", context do
    tenant = context[:tenant]

    user_a =
      TestRepo.insert!(%User{name: "John", inserted_at: ~N[2024-08-31 19:56:55]}, prefix: tenant)

    TestRepo.insert!(%User{id: user_a.id, name: "NotJohn"},
      prefix: tenant,
      on_conflict: :replace_all
    )

    user_b = TestRepo.get(User, user_a.id, prefix: tenant)

    assert user_a.name != user_b.name
    assert user_a.inserted_at != user_b.inserted_at
  end

  test "on_conflict: {:replace_all_except, fields}", context do
    tenant = context[:tenant]

    user_a =
      TestRepo.insert!(
        %User{
          name: "John",
          inserted_at: ~N[2024-08-31 19:56:55],
          updated_at: ~N[2024-08-31 19:56:55]
        },
        prefix: tenant
      )

    TestRepo.insert!(%User{id: user_a.id, name: "NotJohn"},
      prefix: tenant,
      on_conflict: {:replace_all_except, [:inserted_at]}
    )

    user_b = TestRepo.get(User, user_a.id, prefix: tenant)

    assert user_a.name != user_b.name
    assert user_a.inserted_at == user_b.inserted_at
    assert user_a.updated_at != user_b.updated_at
  end

  test "on_conflict: {:replace, fields}", context do
    tenant = context[:tenant]

    user_a =
      TestRepo.insert!(
        %User{
          name: "John",
          inserted_at: ~N[2024-08-31 19:56:55],
          updated_at: ~N[2024-08-31 19:56:55]
        },
        prefix: tenant
      )

    TestRepo.insert!(%User{id: user_a.id, name: "NotJohn"},
      prefix: tenant,
      on_conflict: {:replace, [:name]}
    )

    user_b = TestRepo.get(User, user_a.id, prefix: tenant)

    assert user_a.name != user_b.name
    assert user_a.inserted_at == user_b.inserted_at
    assert user_a.updated_at == user_b.updated_at
  end

  test "on_conflict: keyword list", context do
    tenant = context[:tenant]
    user = TestRepo.insert!(%User{name: "John"}, prefix: tenant)

    assert_raise(
      Unsupported,
      ~r/not supported/,
      fn ->
        TestRepo.insert!(%User{id: user.id, name: "NotJohn"},
          prefix: tenant,
          on_conflict: [set: [name: "foo"]]
        )
      end
    )
  end

  test "on_conflict: Query", context do
    tenant = context[:tenant]
    user = TestRepo.insert!(%User{name: "John"}, prefix: tenant)

    assert_raise(
      Unsupported,
      ~r/not supported/,
      fn ->
        TestRepo.insert!(%User{id: user.id, name: "NotJohn"},
          prefix: tenant,
          on_conflict: from(u in User, update: [set: [name: "foo"]])
        )
      end
    )
  end

  test "conflict_target: []", context do
    tenant = context[:tenant]

    user_a =
      TestRepo.insert!(%User{name: "John", inserted_at: ~N[2024-08-31 19:56:55]}, prefix: tenant)

    # In using `conflict_target: []`, we pretend that the data doesn't exist. This speeds
    # up data loading but can result in inconsistent indexes if objects do exist in
    # the database that are being blindly overwritten. It should be used with extreme caution.
    #
    # `TestRepo.insert!` can be used here but instead we use `TestRepo.async_insert_all!` so that we
    # can exercise that path
    f =
      TestRepo.transactional(tenant, fn ->
        TestRepo.async_insert_all!(User, [%User{id: user_a.id, name: "NotJohn"}],
          conflict_target: []
        )
      end)

    [_user_b] = TestRepo.await(f)

    user_b = TestRepo.get(User, user_a.id, prefix: tenant)

    assert user_a.name != user_b.name
    assert user_a.inserted_at != user_b.inserted_at
  end
end
