defmodule Ecto.Integration.WatchJanitorTest do
  use Ecto.Integration.Case, async: true

  alias Ecto.Adapters.FoundationDB
  alias Ecto.Integration.TestRepo
  alias EctoFoundationDB.Schemas.User
  alias EctoFoundationDB.WatchJanitor

  setup do
    config = [cleanup_interval: :timer.minutes(60), cleanup_check_older_than: 0]
    {:ok, pid} = WatchJanitor.start_link([config], [])
    [janitor: pid]
  end

  test "process down", context do
    tenant = context[:tenant]
    janitor = context[:janitor]

    future =
      TestRepo.transactional(
        tenant,
        fn ->
          alice = TestRepo.insert!(%User{name: "Alice"})
          TestRepo.watch(alice)
        end
      )

    WatchJanitor.register(janitor, self(), [future])
    WatchJanitor.register(janitor, self(), [future])

    assert {:ok, %{mref: mref, futures: [_, _]}} = WatchJanitor.fetch_registered(janitor, self())

    send(janitor, {:DOWN, mref, :process, self(), :normal})

    assert :error = WatchJanitor.fetch_registered(janitor, self())
  end

  test "normal cleanup", context do
    tenant = context[:tenant]
    janitor = context[:janitor]

    {alice, future} =
      TestRepo.transactional(
        tenant,
        fn ->
          alice = TestRepo.insert!(%User{name: "Alice"})
          {alice, TestRepo.watch(alice)}
        end
      )

    WatchJanitor.register(janitor, self(), [future])

    WatchJanitor.cleanup(janitor)

    assert {:ok, %{futures: [_]}} = WatchJanitor.fetch_registered(janitor, self())

    alice
    |> FoundationDB.usetenant(tenant)
    |> User.changeset(%{name: "Alicia"})
    |> TestRepo.update!()

    _ = TestRepo.await(future)

    WatchJanitor.cleanup(janitor)

    assert :error = WatchJanitor.fetch_registered(janitor, self())
  end
end
