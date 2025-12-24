defmodule EctoIntegrationSyncIntegrationTest do
  use Ecto.Integration.Case, async: true

  alias Ecto.Integration.TestRepo
  alias EctoFoundationDB.Schemas.Post
  alias EctoFoundationDB.Schemas.User
  alias EctoFoundationDB.Sync

  import Ecto.Query

  defmodule View do
    use GenServer

    defp sync_opts(),
      do: [
        watch_action: :collection,
        assign: &Sync.assign_map/2,
        attach_container_hook: fn state, _name, _repo, _opts -> state end,
        detach_container_hook: fn state, _name, _repo, _opts -> state end
      ]

    def start_link(tenant, label, id) do
      GenServer.start_link(__MODULE__, [tenant, label, id], [])
    end

    def cancel_user_sync(pid, label) do
      switch_user(pid, label, nil)
    end

    def switch_user(pid, label, id) do
      GenServer.cast(pid, {:switch_user, label, id})
    end

    def await(pid, timeout \\ 100) do
      GenServer.call(pid, :await, timeout)
    end

    def init([tenant, label, id]) do
      state = %{assigns: %{}, changed?: false, waiting: nil, private: %{tenant: tenant}}

      {:ok,
       state
       |> Sync.sync_one(TestRepo, label, User, id, sync_opts())
       |> Sync.sync_all(
         TestRepo,
         :user_collection,
         from(u in User, select: u.id, order_by: u.name),
         sync_opts()
       )
       |> Sync.sync_all_by(TestRepo, :posts, Post, [user_id: id], sync_opts())
       |> Sync.attach_callback(TestRepo, :handle_assigns, &handle_assigns/2)}
    end

    def handle_call(:await, _from, state = %{changed?: true}) do
      {:reply, state.assigns, %{state | changed?: false}}
    end

    def handle_call(:await, from, state) do
      {:noreply, %{state | waiting: from}}
    end

    def handle_cast({:switch_user, label, nil}, state) do
      {:noreply, Sync.cancel(state, TestRepo, label, sync_opts())}
    end

    def handle_cast({:switch_user, label, id}, state) do
      {:noreply,
       state
       |> Sync.sync_one(TestRepo, label, User, id, sync_opts())
       |> Sync.sync_all_by(
         TestRepo,
         :posts,
         Post,
         [user_id: id],
         sync_opts() ++ [watch_action: :collection]
       )}
    end

    def handle_info(msg = {_ref, :ready}, state) do
      {:halt, state} =
        Sync.handle_ready(TestRepo, msg, state, sync_opts())

      {:noreply, state}
    end

    def terminate(_reason, state) do
      Sync.cancel_all(state, TestRepo, sync_opts())
    end

    defp handle_assigns(state, [:user_collection]) do
      id_assigns = for id <- state.assigns.user_collection, do: {[:user_map, id], User, id}

      state
      |> Sync.sync_many(TestRepo, id_assigns, [{:replace, false} | sync_opts()])
      |> notify()
    end

    defp handle_assigns(state, _labels) do
      notify(state)
    end

    defp notify(state) do
      if state.waiting do
        GenServer.reply(state.waiting, state.assigns)
        {:halt, %{state | waiting: nil, changed?: false}}
      else
        {:halt, %{state | changed?: true}}
      end
    end
  end

  test "syncing a record", context do
    tenant = context[:tenant]
    alice = TestRepo.insert!(%User{name: "Alice"}, prefix: tenant)
    {:ok, pid} = View.start_link(tenant, :user, alice.id)

    alice
    |> User.changeset(%{name: "Alicia"})
    |> TestRepo.update!()

    assert %{user: %User{name: "Alicia"}} = View.await(pid)
  end

  test "syncing a nested record", context do
    tenant = context[:tenant]
    alice = TestRepo.insert!(%User{name: "Alice"}, prefix: tenant)
    {:ok, pid} = View.start_link(tenant, [:user, "alice", 1], alice.id)

    alice
    |> User.changeset(%{name: "Alicia"})
    |> TestRepo.update!()

    assert %{user: %{"alice" => %{1 => %User{name: "Alicia"}}}} = View.await(pid)
  end

  test "syncing a collection", context do
    tenant = context[:tenant]
    alice = TestRepo.insert!(%User{name: "Alice"}, prefix: tenant)

    {:ok, pid} = View.start_link(tenant, :user, alice.id)

    _bob = TestRepo.insert!(%User{name: "Bob"}, prefix: tenant)

    assert %{user: %User{name: "Alice"}, user_collection: [_, _]} = View.await(pid)
  end

  test "change the sync target", context do
    tenant = context[:tenant]

    alice = TestRepo.insert!(%User{name: "Alice"}, prefix: tenant)
    bob = TestRepo.insert!(%User{name: "Bob"}, prefix: tenant)

    {:ok, pid} = View.start_link(tenant, :user, alice.id)
    View.switch_user(pid, :user, bob.id)

    assert %{user: %User{name: "Bob"}} = View.await(pid)

    alice
    |> User.changeset(%{name: "Alicia"})
    |> TestRepo.update!()

    bob
    |> User.changeset(%{name: "Robert"})
    |> TestRepo.update!()

    assert %{user: %User{name: "Robert"}} = View.await(pid)
  end

  test "canceling a sync", context do
    tenant = context[:tenant]
    alice = TestRepo.insert!(%User{name: "Alice"}, prefix: tenant)
    {:ok, pid} = View.start_link(tenant, :user, alice.id)

    View.cancel_user_sync(pid, :user)

    alice
    |> User.changeset(%{name: "Alicia"})
    |> TestRepo.update!()

    catch_exit(View.await(pid))
  end

  test "syncing an indexed collection", context do
    tenant = context[:tenant]
    alice = TestRepo.insert!(%User{name: "Alice"}, prefix: tenant)

    {:ok, pid} = View.start_link(tenant, :user, alice.id)

    _ =
      TestRepo.insert(%Post{user: alice, title: "My first post", content: "Hello World"},
        prefix: tenant
      )

    assert %{posts: [_]} = View.await(pid)
  end

  test "sync individual entries from a collection", context do
    tenant = context[:tenant]
    alice = TestRepo.insert!(%User{name: "Alice"}, prefix: tenant)
    bob = TestRepo.insert!(%User{name: "Bob"}, prefix: tenant)

    {:ok, pid} = View.start_link(tenant, :user, alice.id)

    charlie = TestRepo.insert!(%User{name: "Charlie"}, prefix: tenant)

    alice_id = alice.id
    bob_id = bob.id
    charlie_id = charlie.id

    assert %{user_map: %{^alice_id => %User{}, ^bob_id => %User{}, ^charlie_id => %User{}}} =
             View.await(pid)
  end
end
