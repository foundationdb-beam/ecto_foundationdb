defmodule Ecto.Adapters.FoundationDB.Layer.Indexer do
  @moduledoc false
  alias Ecto.Adapters.FoundationDB.Layer.Index
  alias Ecto.Adapters.FoundationDB.Layer.Pack
  alias Ecto.Adapters.FoundationDB.QueryPlan

  @callback create(:erlfdb.transaction(), Index.t(), Ecto.Schema.t()) :: :ok
  @callback set(:erlfdb.transaction(), Index.t(), Ecto.Schema.t(), tuple()) :: :ok
  @callback clear(:erlfdb.transaction(), Index.t(), Ecto.Schema.t(), tuple()) :: :ok
  @callback update(:erlfdb.transaction(), Index.t(), Ecto.Schema.t(), tuple()) :: :ok
  @callback range(Index.t(), QueryPlan.t(), Keyword.t()) :: tuple()
  @callback unpack(Index.t(), QueryPlan.t(), tuple()) :: tuple()
  @optional_callbacks update: 4, unpack: 3

  def create(tx, idx, schema),
    do: idx[:indexer].create(tx, idx, schema)

  def set(tx, idxs, schema, kv) do
    for idx <- idxs,
        do: idx[:indexer].set(tx, idx, schema, kv)
  end

  def clear(tx, idxs, schema, kv) do
    for idx <- idxs,
        do: idx[:indexer].clear(tx, idx, schema, kv)
  end

  def update(tx, idxs, schema, kv) do
    for idx <- idxs do
      apply(
        idx[:indexer],
        :update,
        [tx, idx, schema, kv],
        &_update/4
      )
    end
  end

  def range(idx, plan, options),
    do: idx[:indexer].range(idx, plan, options)

  def unpack(idx, plan, fdb_kv),
    do: apply(idx[:indexer], :unpack, [idx, plan, fdb_kv], &_unpack/3)

  # Default behavior for standard key-value response
  defp _unpack(_idx, _plan, {fdb_key, fdb_value}), do: {fdb_key, Pack.from_fdb_value(fdb_value)}

  # Default behavior for get_mapped_range response
  defp _unpack(idx, plan, {{_pkey, _pvalue}, {_skeybegin, _skeyend}, [fdb_kv]}),
    do: _unpack(idx, plan, fdb_kv)

  defp _unpack(_idx, _plan, {{_pkey, _pvalue}, {_skeybegin, _skeyend}, []}),
    do: nil

  defp _update(tx, idx, schema, kv) do
    idx[:indexer].clear(tx, idx, schema, kv)
    idx[:indexer].set(tx, idx, schema, kv)
  end

  defp apply(module, fun, args, default_fun) do
    if function_exported?(module, fun, length(args)) do
      apply(module, fun, args)
    else
      apply(default_fun, args)
    end
  end
end
