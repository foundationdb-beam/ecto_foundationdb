defmodule EctoFoundationDB.Integration.ClusterHelper do
  @moduledoc false

  alias Ecto.Adapters.FoundationDB.Exception.Unsupported
  alias Ecto.Integration.TestRepo

  alias Ecto.Adapters.FoundationDB.Tenant
  alias Ecto.Adapters.FoundationDB.Transaction

  alias Ecto.Migration.SchemaMigration

  alias EctoFoundationDB.Schemas.User

  import Ecto.Query

  def start(repo) do
    {:ok, pid} = repo.start_link()
    Process.unlink(pid)
    :ok
  end

  def insert_John?(tenant_id) do
    tenant = Tenant.open!(TestRepo, tenant_id, migrator: nil)

    case %User{name: "John"}
         |> TestRepo.insert(prefix: tenant) do
      {:ok, user} ->
        {true, user.id}

      _ ->
        {false, nil}
    end
  end

  def get_John?(tenant_id, user_id) do
    tenant = Tenant.open!(TestRepo, tenant_id, migrator: nil)

    case TestRepo.get(User, user_id, prefix: tenant) do
      nil ->
        false

      user ->
        user.name == "John"
    end
  end

  def migrations_consistent_with_indexes?(tenant_id) do
    tenant = Tenant.open!(TestRepo, tenant_id, migrator: nil)

    # In a transaction, we should read the current migration version and
    # the current index inventory. If there's a race condition, we will
    # be able to read an advanced version with an empty inventory. This
    # represents an invalid state
    {repo, query, from_opts} =
      SchemaMigration.versions(TestRepo, TestRepo.config(), tenant)

    Transaction.commit(tenant, fn ->
      versions = repo.all(query, from_opts)

      ret =
        if versions != [] do
          from(u in User, where: u.name == ^"John")
          |> TestRepo.all(prefix: tenant)
          |> is_list()
        else
          try do
            from(u in User, where: u.name == ^"John")
            |> TestRepo.all(prefix: tenant)
          rescue
            _ in Unsupported ->
              true
          else
            _ ->
              false
          end
        end

      ret
    end)
  end
end
