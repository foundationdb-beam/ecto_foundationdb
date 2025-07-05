defmodule EctoIntegrationWatchTest do
  use Ecto.Integration.Case, async: true

  alias Ecto.Integration.TestRepo
  alias EctoFoundationDB.Schemas.User

  test "watch", context do
    tenant = context[:tenant]

    # We're emulating an Elixir process that keeps track of structs in a map with
    # some custom label.
    assigns = %{mykey: nil}

    # Our process has created a watch and will receive a message when the struct
    # changes.
    {assigns, futures} =
      TestRepo.transactional(
        tenant,
        fn ->
          alice = TestRepo.insert!(%User{name: "Alice"})
          future = TestRepo.watch(alice, label: :mykey)
          {%{assigns | mykey: alice}, [future]}
        end
      )

    assert %User{name: "Alice"} = assigns.mykey

    # This transaction emulates some other change to the DB that is independent of
    # our process. For simplicity, we're using the same tenant ref, but that isn't required.
    {:ok, _alicia} =
      TestRepo.transactional(
        tenant,
        fn ->
          TestRepo.get_by!(User, name: "Alice")
          |> User.changeset(%{name: "Alicia"})
          |> TestRepo.update()
        end
      )

    # Here we emulate our process's event loop (e.g. handle_info). When we receive a {ref, :ready}
    # message, we use TestRepo to retrieve the result according to the previously specified :label.
    # The returned map is merged into our assigns. We also create another watch so that the event loop
    # could continue in the same manner. Instead of looping, we end our test.
    {assigns, futures} =
      receive do
        {ref, :ready} when is_reference(ref) ->
          {ready_assigns, futures} =
            TestRepo.assign_ready(futures, [ref], watch?: true, prefix: tenant)

          assert is_list(ready_assigns)

          {Map.merge(assigns, Enum.into(ready_assigns, %{})), futures}
      after
        100 ->
          raise "Future result not received within 100 msec"
      end

    assert %User{name: "Alicia"} = assigns.mykey
    assert [_] = futures
  end
end
