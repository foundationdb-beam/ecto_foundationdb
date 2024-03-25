defmodule Ecto.Adapters.FoundationDB.Layer.IndexInventory do
  @moduledoc """
  This is an internal module that manages index creation and metadata.
  """
  alias Ecto.Adapters.FoundationDB.Layer.Indexer
  alias Ecto.Adapters.FoundationDB.Layer.Indexer.Default
  alias Ecto.Adapters.FoundationDB.Layer.Indexer.MaxValue
  alias Ecto.Adapters.FoundationDB.Layer.Pack
  alias Ecto.Adapters.FoundationDB.Layer.Tx
  alias Ecto.Adapters.FoundationDB.Migration.SchemaMigration

  @index_inventory_source "\xFFindexes"
  @max_version_name "version"
  @idx_operation_failed {:erlfdb_error, 1020}

  def source(), do: @index_inventory_source

  def builtin_indexes() do
    migration_source = SchemaMigration.source()

    %{
      migration_source => [
        [
          id: @max_version_name,
          indexer: MaxValue,
          source: migration_source,
          fields: [:version],
          options: []
        ]
      ]
    }
  end

  def create_index(
        db_or_tenant,
        source,
        index_name,
        index_fields,
        options
      ) do
    {inventory_key, idx} = new_index(source, index_name, index_fields, options)

    Tx.transactional(db_or_tenant, fn tx ->
      # Write a key that indicates the index exists. All other operations will
      # use this info to maintain the index
      :erlfdb.set(tx, inventory_key, Pack.to_fdb_value(idx))

      Indexer.create(tx, idx)
    end)

    :ok
  end

  @doc """
  Create an FDB kv to be stored in the schema_migrations source. This kv contains
  the information necessary to manage data objects' associated indexes.

  ## Examples

    iex> {key, obj} = Ecto.Adapters.FoundationDB.Layer.IndexInventory.new_index("users", "users_name_index", [:name], [])
    iex> {:erlfdb_tuple.unpack(key), obj}
    {{"\\xFE", "\\xFFindexes", "users", "users_name_index"}, [id: "users_name_index", indexer: Ecto.Adapters.FoundationDB.Layer.Indexer.Default, source: "users", fields: [:name], options: []]}

  """
  def new_index(source, index_name, index_fields, options) do
    inventory_key = Pack.namespaced_pack(source(), source, ["#{index_name}"])

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
  Executes function within a transaction, while also supplying the indexes currently
  existing for the schema.

  This function uses the Ecto cache and clever use of FDB constructs to guarantee
  that the cache is consistent with transactional semantics.
  """
  def transactional(db_or_tenant, %{cache: cache, opts: adapter_opts}, source, fun) do
    cache? = :enabled == Application.get_env(:ecto_foundationdb, :idx_cache, :enabled)
    cache_key = {__MODULE__, db_or_tenant, source}

    Tx.transactional(db_or_tenant, fn tx ->
      tx_with_idxs_cache(tx, cache?, cache, adapter_opts, source, cache_key, fun)
    end)
  end

  defp tx_with_idxs_cache(tx, cache?, cache, adapter_opts, source, cache_key, fun) do
    now = System.monotonic_time(:millisecond)

    {_, {cvsn, cidxs}, ts} = cache_lookup(cache?, cache, cache_key, now)

    {vsn, idxs, validator} = tx_idxs(tx, adapter_opts, source, {cvsn, cidxs})

    cache_update(cache?, cache, cache_key, {cvsn, cidxs}, {vsn, idxs}, ts, now)

    try do
      fun.(tx, idxs)
    after
      unless validator.() do
        :ets.delete(cache, cache_key)
        :erlang.error(@idx_operation_failed)
      end
    end
  end

  defp tx_idxs(tx, adapter_opts, source, cache_val) do
    case Map.get(builtin_indexes(), source, nil) do
      nil ->
        tx_idxs_get(tx, adapter_opts, source, cache_val)

      idxs ->
        {-1, idxs, fn -> true end}
    end
  end

  defp tx_idxs_get(tx, adapter_opts, source, {vsn, idxs}) do
    max_version_future = MaxValue.get(tx, SchemaMigration.source(), @max_version_name)

    case idxs do
      idxs when is_list(idxs) ->
        tx_idxs_try_cache({vsn, idxs}, max_version_future)

      _idxs ->
        tx_idxs_get_wait(tx, adapter_opts, source, max_version_future)
    end
  end

  defp tx_idxs_try_cache({vsn, idxs}, max_version_future) do
    # This validator function will return false if the cached vsn is out of date.
    # We defer its execution via this anonymous function so that the
    # important 'gets' can be waited on first, and this one can be checked
    # at the very end. In this way, we are optimistic that the version
    # will change very infrequently.
    vsn_validator = fn ->
      max_version =
        max_version_future
        |> :erlfdb.wait()
        |> MaxValue.decode()

      max_version <= vsn
    end

    {vsn, idxs, vsn_validator}
  end

  defp tx_idxs_get_wait(tx, _adapter_opts, source, max_version_future) do
    {start_key, end_key} = Pack.namespaced_range(source(), source, [])

    idxs =
      tx
      |> :erlfdb.get_range(start_key, end_key)
      |> :erlfdb.wait()
      |> Enum.map(fn {_, fdb_value} -> Pack.from_fdb_value(fdb_value) end)

    max_version =
      max_version_future
      |> :erlfdb.wait()
      |> MaxValue.decode()

    {max_version, idxs, fn -> true end}
  end

  defp cache_lookup(cache?, cache, cache_key, now) do
    case {cache?, :ets.lookup(cache, cache_key)} do
      {true, [item]} ->
        item

      _ ->
        {cache_key, {-1, nil}, now}
    end
  end

  defp cache_update(cache?, cache, cache_key, {cvsn, cidxs}, {vsn, idxs}, ts, now) do
    cond do
      cache? and vsn >= 0 and {vsn, idxs} != {cvsn, cidxs} ->
        :ets.insert(cache, {cache_key, {vsn, idxs}, System.monotonic_time(:millisecond)})

      cache? and cvsn >= 0 ->
        diff = now - ts
        :ets.update_counter(cache, cache_key, {3, diff})

      true ->
        :ok
    end
  end
end
