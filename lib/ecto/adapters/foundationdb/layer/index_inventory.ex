defmodule Ecto.Adapters.FoundationDB.Layer.IndexInventory do
  @moduledoc """
  This is an internal module that manages index creation and metadata.
  """
  alias Ecto.Adapters.FoundationDB.EctoAdapterMigration
  alias Ecto.Adapters.FoundationDB.Layer.Pack
  alias Ecto.Adapters.FoundationDB.Layer.Tx

  @index_inventory_source "indexes"

  def source(), do: EctoAdapterMigration.prepare_migration_key(@index_inventory_source)

  def create_index(
        db_or_tenant,
        adapter_meta,
        source,
        index_name,
        index_fields,
        options
      ) do
    inventory_kv = new_index(adapter_meta, source, index_name, index_fields, options)

    Tx.transactional(db_or_tenant, fn tx ->
      Tx.create_index(
        tx,
        adapter_meta,
        source,
        index_name,
        index_fields,
        options,
        inventory_kv
      )
    end)

    :ok
  end

  @doc """
  Create an FDB kv to be stored in the schema_migrations source. This kv contains
  the information necessary to manage data objects' associated indexes.

  ## Examples

    iex> {k, v} = Ecto.Adapters.FoundationDB.Layer.IndexInventory.new_index(%{opts: []}, "users", "users_name_index", [:name], [])
    iex> {k, Ecto.Adapters.FoundationDB.Layer.Pack.from_fdb_value(v)}
    {"\\xFEindexes/users/users_name_index", [id: "users_name_index", source: "users", fields: [:name], options: []]}

  """
  def new_index(%{opts: adapter_opts}, source, index_name, index_fields, options) do
    inventory_key = Pack.to_raw_fdb_key(adapter_opts, [source(), source, index_name])

    inventory_value =
      Pack.to_fdb_value(id: index_name, source: source, fields: index_fields, options: options)

    {inventory_key, inventory_value}
  end

  @doc """
  Using a list of fields, usually given by an ecto where clause, select an index from those
  that are available.

  ## Examples

    iex> Ecto.Adapters.FoundationDB.Layer.IndexInventory.select_index([[fields: [:name]]], [:name])
    {:ok, [fields: [:name]]}

    iex> Ecto.Adapters.FoundationDB.Layer.IndexInventory.select_index([[fields: [:name]]], [:department])
    {:error, :no_valid_index}

  """
  def select_index([], _where_fields) do
    {:error, :no_valid_index}
  end

  def select_index([idx | idxs], where_fields) do
    case idx[:fields] do
      ^where_fields ->
        {:ok, idx}

      _ ->
        select_index(idxs, where_fields)
    end
  end

  @doc """
  This function retrieves the indexes that have been created for the given source table.

  These keys change very rarely -- specifically whenever migrations are executed. If we
  can find a safe way to cache them in memory then we can avoid this get/wait in each
  transaction. However, such a cache would need to be properly locked during migrations,
  which can be quite complicated across multiple Elixir nodes.
  """
  def tx_idxs(tx, adapter_opts, source) do
    tx
    |> :erlfdb.get_range_startswith(source_range_startswith(adapter_opts, source))
    |> :erlfdb.wait()
    |> Enum.map(fn {_, fdb_value} -> Pack.from_fdb_value(fdb_value) end)
  end

  @doc """
  ## Examples

  iex> Ecto.Adapters.FoundationDB.Layer.IndexInventory.source_range_startswith([], "users")
  "\\xFEindexes/users/"
  """
  def source_range_startswith(adapter_opts, source) do
    Pack.to_raw_fdb_key(adapter_opts, [source(), source, ""])
  end
end
