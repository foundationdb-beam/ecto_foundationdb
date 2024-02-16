defmodule Ecto.Adapters.FoundationDB.Layer.IndexInventory do
  alias Ecto.Adapters.FoundationDB.EctoAdapterMigration
  alias Ecto.Adapters.FoundationDB.Layer.Pack
  alias Ecto.Adapters.FoundationDB.Layer.Tx

  @index_inventory_source "indexes"

  def source(), do: EctoAdapterMigration.prepare_migration_key(@index_inventory_source)

  def create_index(
        db_or_tenant,
        adapter_meta = %{cache: cache},
        source,
        index_name,
        index_fields,
        options
      ) do
    :ets.delete_all_objects(cache)
    inventory_kv = new_index(adapter_meta, source, index_name, index_fields, options)

    Tx.create_index(
      db_or_tenant,
      adapter_meta,
      source,
      index_name,
      index_fields,
      options,
      inventory_kv
    )
  end

  def new_index(%{opts: adapter_opts}, source, index_name, index_fields, options) do
    inventory_key = Pack.to_raw_fdb_key(adapter_opts, [source(), source, index_name])

    inventory_value =
      Pack.to_fdb_value(id: index_name, source: source, fields: index_fields, options: options)

    {inventory_key, inventory_value}
  end

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

  def get_for_source(%{cache: cache, opts: adapter_opts}, db_or_tenant, source) do
    case :ets.lookup(cache, {__MODULE__, db_or_tenant, source}) do
      [{_, res}] ->
        res

      [] ->
        idxs =
          Tx.transactional(
            db_or_tenant,
            fn tx ->
              tx
              |> :erlfdb.get_range_startswith(source_range_startswith(adapter_opts, source))
              |> :erlfdb.wait()
              |> Enum.map(fn {_, fdb_value} -> Pack.from_fdb_value(fdb_value) end)
            end
          )

        :ets.insert(cache, {{__MODULE__, db_or_tenant, source}, idxs})
        idxs
    end
  end

  def source_range_startswith(adapter_opts, source) do
    Pack.to_raw_fdb_key(adapter_opts, [source(), source, ""])
  end
end
