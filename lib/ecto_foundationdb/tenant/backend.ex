defmodule EctoFoundationDB.Tenant.Backend do
  @moduledoc false
  alias EctoFoundationDB.Database
  alias EctoFoundationDB.Layer.Pack
  alias EctoFoundationDB.Options
  alias EctoFoundationDB.Tenant

  @type txobj() :: :erlfdb.database() | :erlfdb.tenant()
  @type ref() :: any()
  @type db_object() :: any()
  @type opened() :: any()
  @type meta() :: any()
  @type tenant_name() :: :erlfdb.tenant_name()

  @callback txobj(db :: :erlfdb.database(), opened :: opened(), meta :: meta()) :: txobj()
  @callback ref(opened :: opened(), meta :: meta()) :: ref()
  @callback make_meta(opened :: opened()) :: meta()
  @callback list(db :: :erlfdb.database(), options :: Options.t()) :: list(db_object())
  @callback create(db :: :erlfdb.database(), tenant_name :: tenant_name(), options :: Options.t()) ::
              :ok | {:error, :tenant_already_exists}
  @callback delete(db :: :erlfdb.database(), tenant_name :: tenant_name(), options :: Options.t()) ::
              :ok
  @callback get(db :: :erlfdb.database(), tenant_name :: tenant_name(), options :: Options.t()) ::
              {:ok, db_object()} | {:error, :tenant_does_not_exist}
  @callback open(db :: :erlfdb.database(), tenant_name :: tenant_name(), options :: Options.t()) ::
              opened()
  @callback all_data_ranges(meta :: meta()) :: list(tuple())
  @callback get_name(id :: String.t(), options :: Options.t()) :: String.t()
  @callback get_id(db_object :: db_object(), options :: Options.t()) :: String.t()
  @callback extend_tuple(tuple :: tuple(), meta :: meta()) :: tuple()
  @callback extract_tuple(tuple :: tuple(), meta :: meta()) :: tuple()

  @spec db_open!(Database.t(), Tenant.id(), Options.t()) :: Tenant.t()
  def db_open!(db, id, options) do
    :ok = ensure_created(db, id, options)
    db_open(db, id, options)
  end

  @doc """
  If the tenant doesn't exist, create it. Otherwise, no-op.
  """
  @spec ensure_created(Database.t(), Tenant.id(), Options.t()) :: :ok
  def ensure_created(db, id, options) do
    case exists?(db, id, options) do
      true -> :ok
      false -> create(db, id, options)
    end
  end

  @doc """
  Returns true if the tenant exists in the database. False otherwise.
  """
  @spec exists?(Database.t(), Tenant.id(), Options.t()) :: boolean()
  def exists?(db, id, options) do
    case get(db, id, options) do
      {:ok, _} -> true
      {:error, :tenant_does_not_exist} -> false
    end
  end

  @spec db_open(Database.t(), Tenant.id(), Options.t()) :: Tenant.t()
  def db_open(db, id, options) do
    module = get_module(options)
    tenant_name = module.get_name(id, options)
    opened = module.open(db, tenant_name, options)
    meta = module.make_meta(opened)

    %Tenant{
      id: id,
      backend: meta.__struct__,
      ref: module.ref(opened, meta),
      txobj: module.txobj(db, opened, meta),
      meta: meta
    }
  end

  @spec list(Database.t(), Options.t()) :: [Tenant.id()]
  def list(db, options) do
    module = get_module(options)

    list = module.list(db, options)

    for db_object <- list do
      module.get_id(db_object, options)
    end
  end

  @spec create(Database.t(), Tenant.id(), Options.t()) :: :ok
  def create(db, id, options) do
    module = get_module(options)
    tenant_name = module.get_name(id, options)

    module.create(db, tenant_name, options)
  end

  @spec clear(Database.t(), Tenant.id(), Options.t()) :: :ok
  def clear(db, id, options) do
    tenant = db_open(db, id, options)

    ranges =
      get_module(options).all_data_ranges(tenant.meta)

    :erlfdb.transactional(Tenant.txobj(tenant), fn tx ->
      for {start_key, end_key} <- ranges, do: :erlfdb.clear_range(tx, start_key, end_key)
    end)

    :ok
  end

  @spec empty(Database.t(), Tenant.id(), Options.t()) :: :ok
  def empty(db, id, options) do
    tenant = db_open(db, id, options)

    {start_key, end_key} =
      Pack.adapter_repo_range(tenant)

    :erlfdb.transactional(Tenant.txobj(tenant), fn tx ->
      :erlfdb.clear_range(tx, start_key, end_key)
    end)

    :ok
  end

  @spec delete(Database.t(), Tenant.id(), Options.t()) :: :ok | {:error, atom()}
  def delete(db, id, options) do
    module = get_module(options)
    tenant_name = module.get_name(id, options)

    module.delete(db, tenant_name, options)
  end

  def get(db, id, options) do
    module = get_module(options)
    tenant_name = module.get_name(id, options)
    module.get(db, tenant_name, options)
  end

  defp get_module(options) do
    Options.get(options, :tenant_backend)
  end
end
