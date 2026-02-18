defmodule Ecto.Integration.SingleTenantTest do
  # Note: single tenant tests do not get a fresh tenant for each test case, so
  # the tests have to be written in a manner that assumes other data may already
  # exist.
  use ExUnit.Case

  alias Ecto.Integration.SingleTenantTestRepo, as: Repo
  alias EctoFoundationDB.Schemas.User
  alias EctoFoundationDB.Sync

  alias EctoFoundationDB.Exception.IncorrectTenancy

  # Reproduces: https://github.com/foundationdb-beam/ecto_foundationdb/issues/83
  # When a Repo uses both `tenant_id` (single-tenant) and `use EctoFoundationDB.Migrator`,
  # starting the repo causes a deadlock: the SingleTenantRepo GenServer handles the
  # `:open` cast, which calls Tenant.open!/3, which runs migrations. The migration
  # queries call CorrectTenancy.fetch_matching_single_tenant/2 with an explicit
  # prefix, which calls SingleTenantRepo.get!/1 — a synchronous GenServer.call back
  # into the same process that is still executing the cast, causing the process to
  # attempt to call itself.
  defmodule SingleTenantWithMigratorRepo do
    @moduledoc false
    use Ecto.Repo, otp_app: :ecto_foundationdb, adapter: Ecto.Adapters.FoundationDB
    use EctoFoundationDB.Migrator

    defmodule Migration0 do
      @moduledoc false
      alias EctoFoundationDB.Schemas.User
      use EctoFoundationDB.Migration

      @impl true
      def change() do
        [create(index(User, [:name]))]
      end
    end

    @impl true
    def migrations() do
      [{0, Migration0}]
    end
  end

  describe "single tenant with migrator" do
    test "starting a single-tenant repo that has a migrator does not deadlock" do
      # The bug: SingleTenantRepo.init/1 casts :open to itself, then handle_cast
      # calls Tenant.open!/3, which runs migrations. During migration, MigrationsPJ
      # queries with an explicit `prefix: tenant`, triggering CorrectTenancy to call
      # SingleTenantRepo.get!/1 — a synchronous call back into the still-running
      # cast handler, causing "process attempted to call itself".
      Application.put_env(:ecto_foundationdb, SingleTenantWithMigratorRepo,
        open_db: &EctoFoundationDB.Sandbox.open_db/1,
        storage_id: EctoFoundationDB.Sandbox,
        tenant_id: "single-tenant-with-migrator"
      )

      assert {:ok, pid} = SingleTenantWithMigratorRepo.start_link()

      # Also verify the repo is functional after initialization
      assert {:ok, _} = SingleTenantWithMigratorRepo.insert(%User{name: "Alice"})

      # Cleanup
      Supervisor.stop(pid)
    end
  end

  describe "single tenant - schema behaviour" do
    test "insert" do
      assert {:ok, _user1} = Repo.insert(%User{name: "Alice"})
    end

    test "update" do
      assert {:ok, user1} = Repo.insert(%User{name: "Alice"})
      assert {:ok, _} = Repo.update(User.changeset(user1, %{name: "Alicia"}))
    end

    test "delete" do
      assert {:ok, user1} = Repo.insert(%User{name: "Alice"})
      assert {:ok, _} = Repo.delete(user1)
    end

    test "watch" do
      assert {:ok, user1} = Repo.insert(%User{name: "Alice"})
      assert %EctoFoundationDB.Future{} = Repo.watch(user1)
    end

    test "assert single tenant only" do
      assert_raise(IncorrectTenancy, ~r/must not have a tenant defined/, fn ->
        Repo.insert(%User{name: "Alice"}, prefix: :some_term)
      end)
    end
  end

  describe "single tenant - queryable behaviour" do
    test "query" do
      assert {:ok, _user1} = Repo.insert(%User{name: "Alice"})
      assert [_ | _] = Repo.all(User)
    end

    test "query delete" do
      assert {:ok, _user1} = Repo.insert(%User{name: "Alice"})
      assert {_total, _} = Repo.delete_all(User)
    end

    test "query update" do
      assert {:ok, _user1} = Repo.insert(%User{name: "Alice"})
      assert {_total, _} = Repo.update_all(User, set: [name: "Alicia"])
    end

    test "stream" do
      assert {:ok, _user1} = Repo.insert(%User{name: "Alice"})

      assert [_ | _] =
               Repo.stream(User)
               |> Enum.to_list()
    end

    test "all range" do
      assert {_total, _} = Repo.delete_all(User)

      Repo.insert(%User{id: "0001", name: "Alice"})
      Repo.insert(%User{id: "0002", name: "Bob"})
      Repo.insert(%User{id: "0003", name: "Charlie"})

      assert [%User{id: "0001", name: "Alice"}, %User{id: "0002", name: "Bob"}] =
               Repo.all_range(User, "0001", "0003")
    end

    test "assert single tenant only" do
      assert_raise(IncorrectTenancy, ~r//, fn ->
        Repo.get(User, "123", prefix: :some_term)
      end)
    end
  end

  describe "single tenant - other features" do
    test "sync" do
      sync_opts = [
        watch_action: :collection,
        assign: fn state, _std_assigns, _idlist_assigns, _opts -> state end,
        attach_container_hook: fn state, _name, _repo, _opts -> state end,
        detach_container_hook: fn state, _name, _repo, _opts -> state end
      ]

      state = %{private: %{}}
      assert {:ok, user} = Repo.insert(%User{name: "Alice"})
      assert %{} = Sync.sync_one(state, Repo, :alice, User, user.id, sync_opts)
    end
  end
end
