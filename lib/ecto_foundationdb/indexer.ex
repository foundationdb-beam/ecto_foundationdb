defmodule EctoFoundationDB.Indexer do
  @moduledoc """
  Implement this behaviour to create a custom index.

  Each Indexer has access to read, write, and **clear** any and all data in the database.
  A faulty implementation may lead to data loss or corruption.
  """
  alias EctoFoundationDB.Index
  alias EctoFoundationDB.Layer.Metadata
  alias EctoFoundationDB.Layer.PrimaryKVCodec
  alias EctoFoundationDB.QueryPlan
  alias EctoFoundationDB.Tenant

  @callback create_range(Tenant.t(), Index.t()) :: {:erlfdb.key(), :erlfdb.key()}
  @callback drop_ranges(Tenant.t(), Index.t()) ::
              list(:erlfdb.key()) | list({:erlfdb.key(), :erlfdb.key()})
  @callback create(
              Tenant.t(),
              :erlfdb.transaction(),
              Index.t(),
              Ecto.Schema.t(),
              tuple(),
              integer()
            ) ::
              {integer(), {:erlfdb.key(), :erlfdb.key()}}
  @callback set(Tenant.t(), :erlfdb.transaction(), Index.t(), Ecto.Schema.t(), tuple()) :: :ok
  @callback clear(Tenant.t(), :erlfdb.transaction(), Index.t(), Ecto.Schema.t(), tuple()) :: :ok
  @callback update(
              Tenant.t(),
              :erlfdb.transaction(),
              Index.t(),
              Ecto.Schema.t(),
              tuple(),
              Keyword.t()
            ) :: :ok
  @callback range(Index.t(), QueryPlan.t(), Keyword.t()) :: tuple()
  @callback unpack(Index.t(), QueryPlan.t(), tuple()) :: tuple()
  @optional_callbacks update: 6, unpack: 3

  def create_range(tenant, idx),
    do: idx[:indexer].create_range(tenant, idx)

  def drop_ranges(tenant, idx),
    do: idx[:indexer].drop_ranges(tenant, idx)

  def create(tenant, tx, idx, schema, range, limit),
    do: idx[:indexer].create(tenant, tx, idx, schema, range, limit)

  def set(tenant, tx, metadata, schema, kv = {kv_codec, _}) do
    %Metadata{indexes: idxs, partial_indexes: partial_idxs} = metadata
    idxs = idxs ++ filter_partials(partial_idxs, kv_codec.packed, [])

    for idx <- idxs,
        do: idx[:indexer].set(tenant, tx, idx, schema, kv)
  end

  def clear(tenant, tx, metadata, schema, kv = {kv_codec, _}) do
    %Metadata{indexes: idxs, partial_indexes: partial_idxs} = metadata
    idxs = idxs ++ filter_partials(partial_idxs, kv_codec.packed, [])

    for idx <- idxs,
        do: idx[:indexer].clear(tenant, tx, idx, schema, kv)
  end

  def update(tenant, tx, metadata, schema, kv = {kv_codec, _}, updates) do
    %Metadata{indexes: idxs, partial_indexes: partial_idxs} = metadata
    idxs = idxs ++ filter_partials(partial_idxs, kv_codec.packed, [])

    for idx <- idxs do
      apply(
        idx[:indexer],
        :update,
        [tenant, tx, idx, schema, kv, updates],
        &_update/6
      )
    end
  end

  def range(idx, plan, options),
    do: idx[:indexer].range(idx, plan, options)

  def unpack(idx, plan, fdb_kv),
    do: apply(idx[:indexer], :unpack, [idx, plan, fdb_kv], &_unpack/3)

  ## Default behavior for standard key-value response
  defp _unpack(_idx, plan, {fdb_key, fdb_value}) do
    [kv] =
      [{fdb_key, fdb_value}]
      |> PrimaryKVCodec.stream_decode(plan.tenant)
      |> Enum.to_list()

    kv
  end

  # Default behavior for get_mapped_range response
  defp _unpack(_idx, _plan, {{_pkey, _pvalue}, {_skeybegin, _skeyend}, []}),
    do: nil

  defp _unpack(_idx, plan, {{_pkey, _pvalue}, {_skeybegin, _skeyend}, fdb_kvs}) do
    [kv] =
      fdb_kvs
      |> PrimaryKVCodec.stream_decode(plan.tenant)
      |> Enum.to_list()

    kv
  end

  defp _update(tenant, tx, idx, schema, kv, updates) do
    if Keyword.get(idx[:options], :mapped?, true) do
      index_fields = idx[:fields]
      set_data = updates[:set] || []
      clear_data = updates[:clear] || []

      x =
        MapSet.intersection(
          MapSet.new(clear_data ++ Keyword.keys(set_data)),
          MapSet.new(index_fields)
        )

      if MapSet.size(x) == 0 do
        :ok
      else
        __update(tenant, tx, idx, schema, kv, updates)
      end
    else
      __update(tenant, tx, idx, schema, kv, updates)
    end
  end

  defp __update(tenant, tx, idx, schema, kv = {kv_codec, v}, updates) do
    idx[:indexer].clear(tenant, tx, idx, schema, kv)

    kv =
      {kv_codec,
       v
       |> Keyword.merge(updates[:set] || [])
       |> Keyword.drop(updates[:clear] || [])}

    idx[:indexer].set(tenant, tx, idx, schema, kv)
  end

  defp apply(module, fun, args, default_fun) do
    if function_exported?(module, fun, length(args)) do
      apply(module, fun, args)
    else
      apply(default_fun, args)
    end
  end

  defp filter_partials([], _k, acc) do
    Enum.reverse(acc)
  end

  defp filter_partials(
         [{partial_idx, {start_key, cursor_key, _end_key}} | partial_idxs],
         fdb_key,
         acc
       ) do
    if fdb_key >= start_key and fdb_key < cursor_key do
      filter_partials(partial_idxs, fdb_key, [partial_idx | acc])
    else
      filter_partials(partial_idxs, fdb_key, acc)
    end
  end
end
