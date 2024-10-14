defmodule EctoFoundationDB.Tenant do
  @moduledoc """
  This module allows the application to create, open, and delete
  tenants within the FoundationDB database. All transactions require a tenant,
  so any application that uses the Ecto FoundationDB Adapter must use this module.

  EctoFoundationDB supports 2 different backends for the Tenant implementation:

  1. `EctoFoundationDB.Tenant.DirectoryTenant`: The default. Tenants are managed via
     the `:erlfdb_directory` layer. Each tenant is assigned a byte prefix. EctoFDB
     puts that prefix as the first element of every tuple key. If your application wishes
     to write keys directly to the database, it must also respect this prefix.
  2. `EctoFoundationDB.Tenant.ManagedTenant`: Uses the experimental [FoundationDB Tenants](https://apple.github.io/foundationdb/tenants.html).
     This requires `tenant_mode=required_experimental` or `tenant_mode=optional_experimental`
     to be enabled on your FDB system. EctoFDB does not need to prefix keys because it happens
     automatically at the FDB transaction level. If your application wishes to write keys
     directory to the database, there is no prefix to worry about. **Use caution:** FDB Tenants
     are not yet tested with the same rigor as the rest of FDB.

  If you wish to use the experimental ManagedTenant, add this option to your Repo config:

  ```elixir
  config :my_app, MyApp.Repo,
    tenant_backend: EctoFoundationDB.Tenant.ManagedTenant
  ```

  EctoFDB does not support switching between tenant backends on a database. If you want to switch backend,
  you must use a new empty database.
  """

  @derive {Inspect, only: [:id, :ref]}
  defstruct [:id, :backend, :ref, :txobj, :meta]

  alias Ecto.Adapters.FoundationDB, as: FDB

  alias EctoFoundationDB.Migrator
  alias EctoFoundationDB.Options
  alias EctoFoundationDB.Tenant
  alias EctoFoundationDB.Tenant.Backend

  @type t() :: %Tenant{}
  @type id() :: :erlfdb.tenant_name()
  @type prefix() :: String.t()

  def txobj(%Tenant{txobj: txobj}), do: txobj

  @doc """
  Returns true if the tenant already exists in the database.
  """
  @spec exists?(Ecto.Repo.t(), id()) :: boolean()
  def exists?(repo, id) when byte_size(id) > 0,
    do: Backend.exists?(FDB.db(repo), id, repo.config())

  @doc """
  Create a tenant in the database.
  """
  @spec create(Ecto.Repo.t(), id()) :: :ok
  def create(repo, id) when byte_size(id) > 0, do: Backend.create(FDB.db(repo), id, repo.config())

  @doc """
  Clears data in a tenant and then deletes it. If the tenant doesn't exist, no-op.
  """
  @spec clear_delete!(Ecto.Repo.t(), id()) :: :ok
  def clear_delete!(repo, id) when byte_size(id) > 0 do
    options = repo.config()
    db = FDB.db(repo)

    if Backend.exists?(db, id, options) do
      :ok = Backend.clear(db, id, options)
      :ok = Backend.delete(db, id, options)
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
  def open(repo, id, options \\ []) when byte_size(id) > 0 do
    config = Keyword.merge(repo.config(), options)
    tenant = Backend.db_open(FDB.db(repo), id, config)
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
  def open!(repo, id, options \\ []) when byte_size(id) > 0 do
    config = Keyword.merge(repo.config(), options)
    tenant = Backend.db_open!(FDB.db(repo), id, config)
    handle_open(repo, tenant, config)
    tenant
  end

  @doc """
  Helper function to ensure the given tenant exists and then clear
  it of all data, and finally return an open handle. Useful in test code,
  but in production, this would be dangerous.
  """
  @spec open_empty!(Ecto.Repo.t(), id(), Options.t()) :: t()
  def open_empty!(repo, id, options_in \\ []) when byte_size(id) > 0 do
    db = FDB.db(repo)
    options = Keyword.merge(repo.config(), options_in)
    :ok = Backend.ensure_created(db, id, options)
    :ok = Backend.empty(db, id, options)
    open(repo, id, options_in)
  end

  @doc """
  List all tenants in the database. Could be expensive.
  """
  @spec list(Ecto.Repo.t()) :: [id()]
  def list(repo), do: Backend.list(FDB.db(repo), repo.config())

  @doc """
  Clear all data for the given tenant. This cannot be undone.
  """
  @spec clear(Ecto.Repo.t(), id()) :: :ok
  def clear(repo, id) when byte_size(id) > 0, do: Backend.clear(FDB.db(repo), id, repo.config())

  @doc """
  Deletes a tenant from the database permanently. The tenant must
  have no data.
  """
  @spec delete(Ecto.Repo.t(), id()) :: :ok
  def delete(repo, id) when byte_size(id) > 0, do: Backend.delete(FDB.db(repo), id, repo.config())

  def pack(tenant, tuple) when is_tuple(tuple) do
    tuple
    |> tenant.backend.extend_tuple(tenant.meta)
    |> :erlfdb_tuple.pack()
  end

  def unpack(tenant, tuple) do
    tuple
    |> :erlfdb_tuple.unpack()
    |> tenant.backend.extract_tuple(tenant.meta)
  end

  def range(tenant, tuple) when is_tuple(tuple) do
    tuple
    |> tenant.backend.extend_tuple(tenant.meta)
    |> :erlfdb_tuple.range()
  end

  def primary_mapper(tenant) do
    # mapper indexes are offset by the number of elements added by `extend_tuple`
    fn offset ->
      # tuple elements: (head,) prefix, source, namespace, id, get_range
      for(i <- offset..(offset + 3), do: "{V[#{i}]}") ++ ["{...}"]
    end
    |> tenant.backend.extend_tuple(tenant.meta)
  end

  defp handle_open(repo, tenant, options) do
    Migrator.up(repo, tenant, options)
  end
end
