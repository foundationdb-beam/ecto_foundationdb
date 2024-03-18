defmodule Ecto.Adapters.FoundationDB.Layer.Indexer do
  @moduledoc false
  alias Ecto.Adapters.FoundationDB.Layer.Index
  alias Ecto.Adapters.FoundationDB.Layer.Pack
  alias Ecto.Adapters.FoundationDB.QueryPlan

  @callback create(:erlfdb.transaction(), Index.t(), map()) :: :ok
  @callback set(:erlfdb.transaction(), Index.t(), map(), tuple()) :: :ok
  @callback clear(:erlfdb.transaction(), Index.t(), map(), tuple()) :: :ok
  @callback update(:erlfdb.transaction(), Index.t(), map(), tuple()) :: :ok
  @callback range(Index.t(), map(), QueryPlan.t(), Keyword.t()) :: tuple()
  @callback unpack(Index.t(), QueryPlan.t(), tuple()) :: tuple()
  @optional_callbacks update: 4, unpack: 3

  def create(tx, idx, adapter_meta),
    do: idx[:indexer].create(tx, idx, adapter_meta)

  def set(tx, idxs, adapter_meta, kv) do
    for idx <- idxs,
        do: idx[:indexer].set(tx, idx, adapter_meta, kv)
  end

  def clear(tx, idxs, adapter_meta, kv) do
    for idx <- idxs,
        do: idx[:indexer].clear(tx, idx, adapter_meta, kv)
  end

  def update(tx, idxs, adapter_meta, kv) do
    for idx <- idxs do
      apply(
        idx[:indexer],
        :update,
        [tx, idx, adapter_meta, kv],
        &_update/4
      )
    end
  end

  def range(idx, adapter_meta, plan, options),
    do: idx[:indexer].range(idx, adapter_meta, plan, options)

  def unpack(idx, plan, fdb_kv),
    do: apply(idx[:indexer], :unpack, [idx, plan, fdb_kv], &_unpack/3)

  def _unpack(_idx, _plan, {_fdb_key, fdb_value}), do: Pack.from_fdb_value(fdb_value)

  def pack(kv), do: Pack.to_fdb_value(kv)

  defp _update(tx, idx, adapter_meta, kv) do
    idx[:indexer].clear(tx, idx, adapter_meta, kv)
    idx[:indexer].set(tx, idx, adapter_meta, kv)
  end

  defp apply(module, fun, args, default_fun) do
    if function_exported?(module, fun, length(args)) do
      apply(module, fun, args)
    else
      apply(default_fun, args)
    end
  end
end
