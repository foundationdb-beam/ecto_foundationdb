defmodule EctoFoundationDB.Tenant do
  @moduledoc """
  This module allows the application to manage the creation and deletion of
  tenants within the FoundationDB database. All transactions require a tenant,
  so any application that uses the Ecto FoundationDB Adapter must use this module.
  """

  @type t() :: :erlfdb.tenant()
  @type id() :: :erlfdb.tenant_name()
  @type prefix() :: String.t()

  alias Ecto.Adapters.FoundationDB, as: FDB
  alias Ecto.Adapters.FoundationDB.EctoAdapterStorage

  alias EctoFoundationDB.Database
  alias EctoFoundationDB.Migrator
  alias EctoFoundationDB.Options

  @doc """
  Returns true if the tenant already exists in the database.
  """
  @spec exists?(Ecto.Repo.t(), id()) :: boolean()
  def exists?(repo, id), do: exists?(FDB.db(repo), id, repo.config())

  @doc """
  Create a tenant in the database.
  """
  @spec create(Ecto.Repo.t(), id()) :: :ok
  def create(repo, id), do: create(FDB.db(repo), id, repo.config())

  @doc """
  Clears data in a tenant and then deletes it. If the tenant doesn't exist, no-op.
  """
  @spec clear_delete!(Ecto.Repo.t(), id()) :: :ok
  def clear_delete!(repo, id) do
    options = repo.config()
    db = FDB.db(repo)

    if exists?(db, id, options) do
      :ok = clear(db, id, options)
      :ok = delete(db, id, options)
    end

    :ok
  end

  @doc """
  Open a tenant with a repo. With the result returned by this function, the caller can
  do database operations on the tenant's portion of the key-value store.

  The tenant must already exist.

  When opening tenants with a repo, all migrations are automatically performed. This
  can cause open/2 to take a significant amount of time. Tenants can be kept open
  indefinitely, with any number of database transactions issued upon them.
  """
  @spec open(Ecto.Repo.t(), id(), Options.t()) :: t()
  def open(repo, id, options \\ []) do
    config = Keyword.merge(repo.config(), options)
    tenant = db_open(FDB.db(repo), id, config)
    handle_open(repo, tenant, config)
    tenant
  end

  @doc """
  Open a tenant. With the result returned by this function, the caller can
  do database operations on the tenant's portion of the key-value store.

  If the tenant does not exist, it is created.

  When opening tenants with a repo, all migrations are automatically performed. This
  can cause open/2 to take a significant amount of time. Tenants can be kept open
  indefinitely, with any number of database transactions issued upon them.
  """
  @spec open!(Ecto.Repo.t(), id(), Options.t()) :: t()
  def open!(repo, id, options \\ []) do
    config = Keyword.merge(repo.config(), options)
    tenant = db_open!(FDB.db(repo), id, config)
    handle_open(repo, tenant, config)
    tenant
  end

  @doc """
  Helper function to ensure the given tenant exists and then clear
  it of all data, and finally return an open handle. Useful in test code,
  but in production, this would be dangerous.
  """
  @spec open_empty!(Ecto.Repo.t(), id(), Options.t()) :: t()
  def open_empty!(repo, id, options_in \\ []) do
    db = FDB.db(repo)
    options = Keyword.merge(repo.config(), options_in)
    :ok = ensure_created(db, id, options)
    :ok = empty(db, id, options)
    open(repo, id, options_in)
  end

  @doc """
  List all tenants in the database. Could be expensive.
  """
  @spec list(Ecto.Repo.t()) :: [id()]
  def list(repo), do: list(FDB.db(repo), repo.config())

  @doc """
  Clear all data for the given tenant. This cannot be undone.
  """
  @spec clear(Ecto.Repo.t(), id()) :: :ok
  def clear(repo, id), do: clear(FDB.db(repo), id, repo.config())

  @doc """
  Deletes a tenant from the database permanently. The tenant must
  have no data.
  """
  @spec delete(Ecto.Repo.t(), id()) :: :ok
  def delete(repo, id), do: delete(FDB.db(repo), id, repo.config())

  @spec db_open!(Database.t(), id(), Options.t()) :: t()
  def db_open!(db, id, options) do
    :ok = ensure_created(db, id, options)
    db_open(db, id, options)
  end

  @doc """
  If the tenant doesn't exist, create it. Otherwise, no-op.
  """
  @spec ensure_created(Database.t(), id(), Options.t()) :: :ok
  def ensure_created(db, id, options) do
    case exists?(db, id, options) do
      true -> :ok
      false -> create(db, id, options)
    end
  end

  @doc """
  Returns true if the tenant exists in the database. False otherwise.
  """
  @spec exists?(Database.t(), id(), Options.t()) :: boolean()
  def exists?(db, id, options), do: EctoAdapterStorage.tenant_exists?(db, id, options)

  @spec db_open(Database.t(), id(), Options.t()) :: t()
  def db_open(db, id, options), do: EctoAdapterStorage.open_tenant(db, id, options)

  @spec list(Database.t(), Options.t()) :: [id()]
  def list(db, options) do
    for {_k, json} <- EctoAdapterStorage.list_tenants(db, options) do
      %{"name" => %{"printable" => name}} = Jason.decode!(json)
      EctoAdapterStorage.tenant_name_to_id!(name, options)
    end
  end

  @spec create(Database.t(), id(), Options.t()) :: :ok
  def create(db, id, options), do: EctoAdapterStorage.create_tenant(db, id, options)

  @spec clear(Database.t(), id(), Options.t()) :: :ok
  def clear(db, id, options), do: EctoAdapterStorage.clear_tenant(db, id, options)

  @spec empty(Database.t(), id(), Options.t()) :: :ok
  def empty(db, id, options), do: EctoAdapterStorage.empty_tenant(db, id, options)

  @spec delete(Database.t(), id(), Options.t()) :: :ok
  def delete(db, id, options), do: EctoAdapterStorage.delete_tenant(db, id, options)

  defp handle_open(repo, tenant, options) do
    Migrator.up(repo, tenant, options)
  end
end
