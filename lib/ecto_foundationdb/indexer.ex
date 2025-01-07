defmodule EctoFoundationDB.Indexer do
  @moduledoc """
  Implement this behaviour to create a custom index.
  """
  alias EctoFoundationDB.Index
  alias EctoFoundationDB.QueryPlan
  alias EctoFoundationDB.Tenant
  alias EctoFoundationDB.Layer.DecodedKV
  alias EctoFoundationDB.Layer.PrimaryKVCodec
  alias EctoFoundationDB.Layer.Pack

  @callback create_range(Tenant.t(), Index.t()) :: {:erlfdb.key(), :erlfdb.key()}
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

  def create(tenant, tx, idx, schema, range, limit),
    do: idx[:indexer].create(tenant, tx, idx, schema, range, limit)

  def set(tenant, tx, idxs, partial_idxs, schema, kv = {k, _}) do
    idxs = idxs ++ filter_partials(partial_idxs, k, [])

    for idx <- idxs,
        do: idx[:indexer].set(tenant, tx, idx, schema, kv)
  end

  def clear(tenant, tx, idxs, partial_idxs, schema, kv = {k, _}) do
    idxs = idxs ++ filter_partials(partial_idxs, k, [])

    for idx <- idxs,
        do: idx[:indexer].clear(tenant, tx, idx, schema, kv)
  end

  def update(tenant, tx, idxs, partial_idxs, schema, kv = {k, _}, updates) do
    idxs = idxs ++ filter_partials(partial_idxs, k, [])

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
  defp _unpack(_idx, plan, {fdb_key, fdb_value}),
    do: %DecodedKV{
      codec: Pack.primary_write_key_to_codec(plan.tenant, fdb_key),
      data_object: Pack.from_fdb_value(fdb_value)
    }

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
      set_data = updates[:set]

      x = MapSet.intersection(MapSet.new(Keyword.keys(set_data)), MapSet.new(index_fields))

      if MapSet.size(x) == 0 do
        :ok
      else
        __update(tenant, tx, idx, schema, kv, updates)
      end
    else
      __update(tenant, tx, idx, schema, kv, updates)
    end
  end

  defp __update(tenant, tx, idx, schema, kv = {k, v}, updates) do
    set_data = updates[:set]
    idx[:indexer].clear(tenant, tx, idx, schema, kv)
    kv = {k, Keyword.merge(v, set_data)}
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
