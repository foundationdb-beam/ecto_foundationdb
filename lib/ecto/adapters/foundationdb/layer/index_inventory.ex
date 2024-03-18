defmodule Ecto.Adapters.FoundationDB.Layer.IndexInventory do
  @moduledoc """
  This is an internal module that manages index creation and metadata.
  """
  alias Ecto.Adapters.FoundationDB.Layer.Indexer
  alias Ecto.Adapters.FoundationDB.Layer.Indexer.Default
  alias Ecto.Adapters.FoundationDB.Layer.Pack
  alias Ecto.Adapters.FoundationDB.Layer.Tx

  @index_inventory_source "indexes"

  def source(), do: Pack.to_fdb_migrationsource(@index_inventory_source)

  def create_index(
        db_or_tenant,
        adapter_meta,
        source,
        index_name,
        index_fields,
        options
      ) do
    {inventory_key, idx} = new_index(adapter_meta, source, index_name, index_fields, options)

    Tx.transactional(db_or_tenant, fn tx ->
      # Write a key that indicates the index exists. All other operations will
      # use this info to maintain the index
      :erlfdb.set(tx, inventory_key, Pack.to_fdb_value(idx))

      Indexer.create(tx, idx, adapter_meta)
    end)

    :ok
  end

  @doc """
  Create an FDB kv to be stored in the schema_migrations source. This kv contains
  the information necessary to manage data objects' associated indexes.

  ## Examples

    iex> Ecto.Adapters.FoundationDB.Layer.IndexInventory.new_index(%{opts: []}, "users", "users_name_index", [:name], [])
    {"\\xFE\\xFFindexes/users/users_name_index", [id: "users_name_index", indexer: Ecto.Adapters.FoundationDB.Layer.Indexer.Default, source: "users", fields: [:name], options: []]}

  """
  def new_index(%{opts: adapter_opts}, source, index_name, index_fields, options) do
    inventory_key = Pack.to_raw_fdb_key(adapter_opts, [source(), source, index_name])

    idx = [
      id: index_name,
      indexer: get_indexer(options),
      source: source,
      fields: index_fields,
      options: options
    ]

    {inventory_key, idx}
  end

  defp get_indexer(options) do
    case Keyword.get(options, :indexer) do
      :timeseries -> Default
      nil -> Default
      module -> module
    end
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
    key = source_range_startswith(adapter_opts, source)

    tx
    |> :erlfdb.get_range_startswith(key)
    |> :erlfdb.wait()
    |> Enum.map(fn {_, fdb_value} -> Pack.from_fdb_value(fdb_value) end)
  end

  @doc """
  ## Examples

  iex> Ecto.Adapters.FoundationDB.Layer.IndexInventory.source_range_startswith([], "users")
  "\\xFE\\xFFindexes/users/"
  """
  def source_range_startswith(adapter_opts, source) do
    Pack.to_raw_fdb_key(adapter_opts, [source(), source, ""])
  end
end
