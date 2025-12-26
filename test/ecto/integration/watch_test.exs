defmodule EctoIntegrationWatchTest do
  use Ecto.Integration.Case, async: true

  alias Ecto.Integration.TestRepo
  alias EctoFoundationDB.Future
  alias EctoFoundationDB.Schemas.User
  alias EctoFoundationDB.Tenant

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

    [watch_future] = futures
    watch_ref = Future.ref(watch_future)

    # Here we emulate our process's event loop (e.g. handle_info). When we receive a {ref, :ready}
    # message, we use TestRepo to retrieve the result according to the previously specified :label.
    # The returned map is merged into our assigns. We also create another watch so that the event loop
    # could continue in the same manner. Instead of looping, we end our test.
    {assigns, futures} =
      receive do
        {^watch_ref, :ready} when is_reference(watch_ref) ->
          {ready_assigns, new_futures, other_futures} =
            TestRepo.assign_ready(futures, [watch_ref], watch?: true, prefix: tenant)

          assert [_] = ready_assigns
          assert is_list(ready_assigns)

          {Map.merge(assigns, Enum.into(ready_assigns, %{})), new_futures ++ other_futures}
      after
        100 ->
          raise "Future result not received within 100 msec"
      end

    assert [_] = futures
    refute watch_future == hd(futures)

    assert %User{name: "Alicia"} = assigns.mykey
    assert %Tenant{} = assigns.mykey.__meta__.prefix
  end

  test "on delete, replace with nil", context do
    tenant = context[:tenant]

    assigns = %{mykey: nil}

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

    [watch_future] = futures
    watch_ref = Future.ref(watch_future)

    TestRepo.delete!(assigns.mykey, prefix: tenant)

    {assigns, _futures} =
      receive do
        {^watch_ref, :ready} when is_reference(watch_ref) ->
          {ready_assigns, new_futures, other_futures} =
            TestRepo.assign_ready(futures, [watch_ref], watch?: true, prefix: tenant)

          assert [_] = ready_assigns
          assert is_list(ready_assigns)

          {Map.merge(assigns, Enum.into(ready_assigns, %{})), new_futures ++ other_futures}
      after
        100 ->
          raise "Future result not received within 100 msec"
      end

    assert {:ok, nil} = Map.fetch(assigns, :mykey)
  end
end
