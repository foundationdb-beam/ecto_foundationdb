defmodule EctoIntegrationPipelineTest do
  use Ecto.Integration.Case, async: true

  import Ecto.Query

  alias Ecto.Integration.TestRepo
  alias EctoFoundationDB.Schemas.User

  test "pipelining", context do
    tenant = context[:tenant]

    ts = NaiveDateTime.utc_now() |> NaiveDateTime.truncate(:second)
    template = %{name: "", inserted_at: ts, updated_at: ts}

    {2, nil} =
      TestRepo.transactional(
        tenant,
        fn ->
          users = [%{template | name: "John"}, %{template | name: "James"}]
          TestRepo.insert_all(User, users)
        end
      )

    john = TestRepo.get_by!(User, [name: "John"], prefix: tenant)
    james = TestRepo.get_by!(User, [name: "James"], prefix: tenant)

    [john, james] =
      TestRepo.transactional(
        tenant,
        fn ->
          futures =
            [f1, f2] = [
              TestRepo.async_get(User, john.id),
              TestRepo.async_get(User, james.id)
            ]

          assert nil != f1.ref
          assert nil != f2.ref
          assert nil == f1.result
          assert nil == f2.result

          TestRepo.await(futures)
        end
      )

    assert john.name == "John"
    assert james.name == "James"

    [john, james] =
      TestRepo.transactional(
        tenant,
        fn ->
          futures =
            [f1, f2] = [
              TestRepo.async_get_by(User, name: "John"),
              TestRepo.async_get_by(User, name: "James")
            ]

          assert nil != f1.ref
          assert nil != f2.ref
          assert nil == f1.result
          assert nil == f2.result

          TestRepo.await(futures)
        end
      )

    assert john.name == "John"
    assert james.name == "James"

    [all_john, all_james] =
      TestRepo.transactional(
        tenant,
        fn ->
          futures =
            [f1, f2] = [
              TestRepo.async_all(from(u in User, where: u.name == ^"John")),
              TestRepo.async_all(from(u in User, where: u.name == ^"James"))
            ]

          assert nil != f1.ref
          assert nil != f2.ref
          assert nil == f1.result
          assert nil == f2.result

          TestRepo.await(futures)
        end
      )

    assert hd(all_john).name == "John"
    assert hd(all_james).name == "James"

    [john, james] =
      TestRepo.transactional(
        tenant,
        fn ->
          futures =
            [f1, f2] = [
              TestRepo.async_one(from(u in User, where: u.name == ^"John")),
              TestRepo.async_one(from(u in User, where: u.name == ^"James"))
            ]

          assert nil != f1.ref
          assert nil != f2.ref
          assert nil == f1.result
          assert nil == f2.result

          TestRepo.await(futures)
        end
      )

    assert john.name == "John"
    assert james.name == "James"
  end

  test "safe insert without conflict_target", context do
    # The implementation of this test is identical to Repo.insert_all,
    # but we include it here as a nontrivial example of several EctoFDB
    # features working together.
    #
    #  - Transactions: FDB transactions are ACID and with serializable isolation
    #  - Pipelining: async_get is used to efficiently check for the existence of
    #    the data.
    #  - Upsert with conflict_target: conflict_target is ignored (with `[]`)
    #    because we've manually confirmed the data does not exist in the DB
    #

    tenant = context[:tenant]

    # Here is the data we wish to load into the DB
    users = [
      %User{id: Ecto.UUID.autogenerate(), name: "John"},
      %User{id: Ecto.UUID.autogenerate(), name: "James"}
    ]

    # Here is the nontrival transaction that we are testing
    load_fn = fn ->
      TestRepo.transactional(
        tenant,
        fn ->
          nils =
            for(u <- users, do: TestRepo.async_get(User, u.id))
            |> TestRepo.await()

          if Enum.all?(nils, &is_nil/1) do
            for(u <- users, do: TestRepo.insert!(u, conflict_target: []))
          else
            raise "Conflict"
          end
        end
      )
    end

    # The first time we call load_fn, it inserts the data
    [john, james] = load_fn.()

    assert john.name == "John"
    assert james.name == "James"

    # The second time, it detects the conflict
    assert_raise(RuntimeError, ~r/Conflict/, load_fn)
  end
end
