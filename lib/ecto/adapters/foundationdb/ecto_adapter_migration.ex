defmodule Ecto.Adapters.FoundationDB.EctoAdapterMigration do
  @moduledoc """
  Ecto.Adapter.Migration lives in ecto_sql, so it has SQL behavior baked in.
  We'll do our best to translate into FoundationDB operations so that migrations
  are minimally usable. The experience might feel a little strange though.
  """
  @behaviour Ecto.Adapter.Migration

  alias Ecto.Adapters.FoundationDB, as: FDB
  alias Ecto.Adapters.FoundationDB.Tenant
  alias Ecto.Adapters.FoundationDB.Layer.IndexInventory
  # alias Ecto.Adapters.FoundationDB.Layer.Tx

  @migration_keyspace_prefix <<0xFE>>

  def prepare_source(k = "schema_migrations"),
    do: {:ok, {prepare_migration_key(k), [usetenant: true]}}

  def prepare_source(_k), do: {:error, :unknown_source}

  def prepare_migration_key(key), do: "#{@migration_keyspace_prefix <> key}"

  @impl true
  def supports_ddl_transaction?() do
    # TODO: maybe support this?
    false
  end

  @impl true
  def execute_ddl(
        _adapter_meta = %{opts: _adapter_opts},
        {:create_if_not_exists,
         %Ecto.Migration.Table{
           name: :schema_migrations,
           primary_key: true
         },
         [
           {:add, :version, :bigint, [primary_key: true]},
           {:add, :inserted_at, :naive_datetime, []}
         ]},
        _options
      ) do
    {:ok, [{}]}
  end

  def execute_ddl(
        adapter_meta = %{opts: adapter_opts},
        {:create,
         %Ecto.Migration.Index{
           prefix: tenant_id,
           table: source,
           name: index_name,
           columns: index_fields
         }},
        _options
      )
      when is_binary(tenant_id) do
    db = FDB.db(adapter_opts)
    tenant = Tenant.open!(db, tenant_id, adapter_opts)
    :ok = IndexInventory.create_index(tenant, adapter_meta, source, index_name, index_fields)
    {:ok, []}
  end

  @impl true
  def lock_for_migrations(_adapter_meta = %{opts: _adapter_opts}, _options, fun) do
    # Ecto locks the `schema_migrations` table when running
    # migrations, guaranteeing two different servers cannot run the same
    # migration at the same time.
    #
    # For FoundationDB, we can write a key in schema_migrations_lock for each
    # tenant. Then, while migrations run, for each tenant being migrated, there
    # are 2 simulatanous transaction. One is a "get" and "clear" on the schema_migration_lock
    # for the tenant, which enters a receive state before the clear. The other
    # is the transaction for the actual migration work being done. When the work
    # is finished, it signals the sleeping transaction to complete.
    # db = FDB.db(adapter_opts)
    # ids = Tenant.list(db, adapter_opts)

    fun.()
  end
end
