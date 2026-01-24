defmodule View do
  use GenServer

  alias Ecto.Integration.TestRepo
  alias EctoFoundationDB.Schemas.Post
  alias EctoFoundationDB.Schemas.User
  alias EctoFoundationDB.Sync

  import Ecto.Query

  defp sync_opts(),
    do: [
      watch_action: :collection,
      assign: &assign/4,
      attach_container_hook: fn state, _name, _repo, _opts -> state end,
      detach_container_hook: fn state, _name, _repo, _opts -> state end
    ]

  defp assign(state, std_assigns, idlist_assigns, opts) do
    assigns = Map.get(state, :assigns, %{})
    new_assigns_map = Sync.assign_map(assigns, std_assigns, idlist_assigns, opts)
    Map.put(state, :assigns, Map.merge(assigns, new_assigns_map))
  end

  def start_link(tenant, label, id) do
    GenServer.start_link(__MODULE__, [self(), tenant, label, id], [])
  end

  def cancel_user_sync(pid, label) do
    switch_user(pid, label, nil)
  end

  def switch_user(pid, label, id) do
    GenServer.cast(pid, {:switch_user, label, id})
  end

  def get_assigns(pid) do
    GenServer.call(pid, :get_assigns)
  end

  def init([listener, tenant, label, id]) do
    state = %{assigns: %{}, changed?: false, listener: listener, private: %{tenant: tenant}}

    {:ok,
     state
     |> Sync.attach_callback(TestRepo, :handle_assigns, &handle_assigns/2)
     |> Sync.sync_one(TestRepo, label, User, id, sync_opts())
     |> Sync.sync_all_by(TestRepo, :posts, Post, [user_id: id], sync_opts())
     |> Sync.sync_all(
       TestRepo,
       :user_collection,
       from(u in User, select: u.id, order_by: u.name),
       sync_opts()
     )}
  end

  def handle_call(:get_assigns, _from, state) do
    {:reply, state.assigns, state}
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

  defp handle_assigns(state, old = %{user_collection: _}) do
    state
    |> Sync.sync_many(TestRepo, :user_list, User, state.assigns.user_collection, sync_opts())
    |> notify(old)
  end

  defp handle_assigns(state, old) do
    if map_size(old) == 0, do: raise("Empty old assigns")
    notify(state, old)
  end

  defp notify(state, old_assigns) do
    %{listener: listener, assigns: assigns} = state
    new_assigns = Map.take(assigns, Map.keys(old_assigns))
    event = {:assigns_changed, old_assigns, new_assigns}
    send(listener, event)
    {:halt, state}
  end

  def flush_assigns_changed(acc, timeout \\ 200) do
    receive do
      event = {:assigns_changed, _old, _new} ->
        flush_assigns_changed([event | acc])
    after
      timeout ->
        Enum.reverse(acc)
    end
  end
end

defmodule EctoIntegrationSyncIntegrationTest1 do
  use Ecto.Integration.Case, async: true

  alias Ecto.Integration.TestRepo
  alias EctoFoundationDB.Exception.IncorrectTenancy
  alias EctoFoundationDB.Schemas.Post
  alias EctoFoundationDB.Schemas.User

  test "syncing a record", context do
    tenant = context[:tenant]
    alice = TestRepo.insert!(%User{name: "Alice"}, prefix: tenant)
    {:ok, pid} = View.start_link(tenant, :user, alice.id)

    alice
    |> User.changeset(%{name: "Alicia"})
    |> TestRepo.update!()

    _ = View.flush_assigns_changed([])
    assert %{user: %User{name: "Alicia"}} = View.get_assigns(pid)
  end

  test "exception raised when tenant not provided", context do
    sync_opts = [
      watch_action: :collection,
      assign: fn state, _std_assigns, _idlist_assigns, _opts -> state end,
      attach_container_hook: fn state, _name, _repo, _opts -> state end,
      detach_container_hook: fn state, _name, _repo, _opts -> state end
    ]

    state = %{private: %{}}
    assert {:ok, user} = TestRepo.insert(%User{name: "Alice"}, prefix: context[:tenant])

    assert_raise IncorrectTenancy,
                 ~r/expecting the provided `:private` map to include a `:tenant`/,
                 fn ->
                   EctoFoundationDB.Sync.sync_one(
                     state,
                     TestRepo,
                     :alice,
                     TestUser,
                     user.id,
                     sync_opts
                   )
                 end
  end
end

defmodule EctoIntegrationSyncIntegrationTest2 do
  use Ecto.Integration.Case, async: true

  alias Ecto.Integration.TestRepo
  alias EctoFoundationDB.Schemas.Post
  alias EctoFoundationDB.Schemas.User

  test "syncing a collection", context do
    tenant = context[:tenant]
    alice = TestRepo.insert!(%User{name: "Alice"}, prefix: tenant)

    {:ok, pid} = View.start_link(tenant, :user, alice.id)

    _bob = TestRepo.insert!(%User{name: "Bob"}, prefix: tenant)

    _ = View.flush_assigns_changed([])
    assert %{user: %User{name: "Alice"}, user_collection: [_, _]} = View.get_assigns(pid)
  end
end

defmodule EctoIntegrationSyncIntegrationTest3 do
  use Ecto.Integration.Case, async: true

  alias Ecto.Integration.TestRepo
  alias EctoFoundationDB.Schemas.Post
  alias EctoFoundationDB.Schemas.User

  test "change the sync target", context do
    tenant = context[:tenant]

    alice = TestRepo.insert!(%User{name: "Alice"}, prefix: tenant)
    bob = TestRepo.insert!(%User{name: "Bob"}, prefix: tenant)

    {:ok, pid} = View.start_link(tenant, :user, alice.id)
    View.switch_user(pid, :user, bob.id)

    _ = View.flush_assigns_changed([])
    assert %{user: %User{name: "Bob"}} = View.get_assigns(pid)

    alice
    |> User.changeset(%{name: "Alicia"})
    |> TestRepo.update!()

    bob
    |> User.changeset(%{name: "Robert"})
    |> TestRepo.update!()

    _ = View.flush_assigns_changed([])
    assert %{user: %User{name: "Robert"}} = View.get_assigns(pid)
  end
end

defmodule EctoIntegrationSyncIntegrationTest4 do
  use Ecto.Integration.Case, async: true

  alias Ecto.Integration.TestRepo
  alias EctoFoundationDB.Schemas.Post
  alias EctoFoundationDB.Schemas.User

  test "canceling a sync", context do
    tenant = context[:tenant]
    alice = TestRepo.insert!(%User{name: "Alice"}, prefix: tenant)
    {:ok, pid} = View.start_link(tenant, :user, alice.id)

    View.cancel_user_sync(pid, :user)

    alice
    |> User.changeset(%{name: "Alicia"})
    |> TestRepo.update!()

    _ = View.flush_assigns_changed([])
    assert %{user: nil} = View.get_assigns(pid)
  end
end

defmodule EctoIntegrationSyncIntegrationTest5 do
  use Ecto.Integration.Case, async: true

  alias Ecto.Integration.TestRepo
  alias EctoFoundationDB.Schemas.Post
  alias EctoFoundationDB.Schemas.User

  test "syncing an indexed collection", context do
    tenant = context[:tenant]
    alice = TestRepo.insert!(%User{name: "Alice"}, prefix: tenant)

    {:ok, pid} = View.start_link(tenant, :user, alice.id)

    _ =
      TestRepo.insert(%Post{user: alice, title: "My first post", content: "Hello World"},
        prefix: tenant
      )

    _ = View.flush_assigns_changed([])
    assert %{posts: [_]} = View.get_assigns(pid)
  end
end

defmodule EctoIntegrationSyncIntegrationTest6 do
  use Ecto.Integration.Case, async: true

  alias Ecto.Integration.TestRepo
  alias EctoFoundationDB.Schemas.User

  test "sync individual entries from a collection", context do
    tenant = context[:tenant]
    alice = TestRepo.insert!(%User{name: "Alice"}, prefix: tenant)
    bob = TestRepo.insert!(%User{name: "Bob"}, prefix: tenant)

    {:ok, pid} = View.start_link(tenant, :user, alice.id)

    alice_id = alice.id
    bob_id = bob.id

    _ = View.flush_assigns_changed([])

    assert %{user: %User{name: "Alice"}, user_collection: [^alice_id, ^bob_id]} =
             View.get_assigns(pid)

    charlie = TestRepo.insert!(%User{name: "Charlie", notes: "foo"}, prefix: tenant)

    charlie_id = charlie.id

    _ = View.flush_assigns_changed([])

    assert %{
             user: %User{name: "Alice"},
             user_list: [%{id: ^alice_id}, %{id: ^bob_id}, %{id: ^charlie_id, notes: "foo"}]
           } =
             View.get_assigns(pid)

    User.changeset(charlie, %{notes: "bar"})
    |> TestRepo.update!()

    _ = View.flush_assigns_changed([])

    assert %{
             user: %User{name: "Alice"},
             user_list: [%{id: ^alice_id}, %{id: ^bob_id}, %{id: ^charlie_id, notes: "bar"}]
           } =
             View.get_assigns(pid)
  end
end

defmodule EctoIntegrationSyncIntegrationTest7 do
  use Ecto.Integration.Case, async: true

  alias Ecto.Integration.TestRepo
  alias EctoFoundationDB.Schemas.User

  test "deleting a record", context do
    tenant = context[:tenant]
    alice = TestRepo.insert!(%User{name: "Alice"}, prefix: tenant)

    {:ok, pid} = View.start_link(tenant, :user, alice.id)

    TestRepo.delete!(alice)

    _ = View.flush_assigns_changed([])
    assert %{user: nil} = View.get_assigns(pid)
  end
end
