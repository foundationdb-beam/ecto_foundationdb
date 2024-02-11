defmodule Ecto.Adapters.FoundationDB do
  @moduledoc """
  Documentation for `Ecto.Adapters.FoundationDB`.
  """

  @behaviour Ecto.Adapter
  @behaviour Ecto.Adapter.Storage
  @behaviour Ecto.Adapter.Schema
  @behaviour Ecto.Adapter.Queryable
  @behaviour Ecto.Adapter.Migration

  alias Ecto.Adapters.FoundationDB.Exception.Unsupported

  alias Ecto.Adapters.FoundationDB.EctoAdapter
  alias Ecto.Adapters.FoundationDB.EctoAdapterStorage
  alias Ecto.Adapters.FoundationDB.EctoAdapterSchema
  alias Ecto.Adapters.FoundationDB.EctoAdapterQueryable
  alias Ecto.Adapters.FoundationDB.EctoAdapterMigration

  def db(repo) when is_atom(repo) do
    db(repo.config())
  end

  def db(options) do
    case :persistent_term.get({__MODULE__, :database}, nil) do
      nil ->
        db = EctoAdapterStorage.open_db(options)
        :persistent_term.put({__MODULE__, :database}, {db, options})
        db

      {db, ^options} ->
        db

      {_db, up_options} ->
        raise Unsupported, """
        FoundationDB Adapater was started with options

        #{inspect(up_options)}

        But since then, options have change to

        #{inspect(options)}
        """
    end
  end

  def usetenant(struct, tenant) do
    Ecto.put_meta(struct, prefix: tenant)
  end

  @impl Ecto.Adapter
  defmacro __before_compile__(_env), do: :ok

  @impl Ecto.Adapter
  defdelegate ensure_all_started(config, type), to: EctoAdapter

  @impl Ecto.Adapter
  defdelegate init(config), to: EctoAdapter

  @impl Ecto.Adapter
  defdelegate checkout(data, config, fun), to: EctoAdapter

  @impl Ecto.Adapter
  defdelegate checked_out?(data), to: EctoAdapter

  @impl Ecto.Adapter
  defdelegate loaders(primitive_type, ecto_type), to: EctoAdapter

  @impl Ecto.Adapter
  defdelegate dumpers(primitive_type, ecto_type), to: EctoAdapter

  @impl Ecto.Adapter.Storage
  defdelegate storage_up(options), to: EctoAdapterStorage

  @impl Ecto.Adapter.Storage
  defdelegate storage_down(options), to: EctoAdapterStorage

  @impl Ecto.Adapter.Storage
  defdelegate storage_status(options), to: EctoAdapterStorage

  @impl Ecto.Adapter.Schema
  defdelegate autogenerate(type), to: EctoAdapterSchema

  @impl Ecto.Adapter.Schema
  defdelegate insert_all(
                adapter_meta,
                schema_meta,
                header,
                unsure,
                on_conflict,
                returning,
                placeholders,
                options
              ),
              to: EctoAdapterSchema

  @impl Ecto.Adapter.Schema
  defdelegate insert(adapter_meta, schema_meta, fields, on_conflict, returning, options),
    to: EctoAdapterSchema

  @impl Ecto.Adapter.Schema
  defdelegate update(adapter_meta, schema_meta, fields, filters, returning, options),
    to: EctoAdapterSchema

  @impl Ecto.Adapter.Schema
  defdelegate delete(adapter_meta, schema_meta, filters, returning, options),
    to: EctoAdapterSchema

  @impl Ecto.Adapter.Queryable
  defdelegate prepare(atom, query), to: EctoAdapterQueryable

  @impl Ecto.Adapter.Queryable
  defdelegate execute(adapter_meta, query_meta, query_cache, params, options),
    to: EctoAdapterQueryable

  @impl Ecto.Adapter.Queryable
  defdelegate stream(adapter_meta, query_meta, query_cache, params, options),
    to: EctoAdapterQueryable

  @impl Ecto.Adapter.Migration
  defdelegate supports_ddl_transaction?(), to: EctoAdapterMigration

  @impl Ecto.Adapter.Migration
  defdelegate execute_ddl(adapter_meta, command, option), to: EctoAdapterMigration

  @impl Ecto.Adapter.Migration
  defdelegate lock_for_migrations(adapter_meta, options, fun), to: EctoAdapterMigration
end
