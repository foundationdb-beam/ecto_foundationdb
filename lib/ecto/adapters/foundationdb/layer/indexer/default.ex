defmodule Ecto.Adapters.FoundationDB.Layer.Indexer.Default do
  @moduledoc false
  alias Ecto.Adapters.FoundationDB.Exception.Unsupported
  alias Ecto.Adapters.FoundationDB.Layer.Indexer
  alias Ecto.Adapters.FoundationDB.Layer.Pack
  alias Ecto.Adapters.FoundationDB.QueryPlan

  @behaviour Indexer

  @doc """
  In the index key, values must be encoded into a fixed-length binary.

  Fixed-length is required so that get_range can be used reliably in the presence of
  arbitrary data.

  However, this means our indexes will have conflicts that must be resolved with
  filtering.

  ## Examples

    iex> Ecto.Adapters.FoundationDB.Layer.Indexer.Default.indexkey_encoder("an-example-key")
    <<37, 99, 56, 165>>

    iex> Ecto.Adapters.FoundationDB.Layer.Indexer.Default.indexkey_encoder(~N[2024-03-01 12:34:56], indexer: :timeseries)
    "20240301T123456.000000"

  """
  def indexkey_encoder(x, index_options \\ []) do
    indexkey_encoder(x, 4, index_options)
  end

  @doc """
  ## Examples

    iex> Ecto.Adapters.FoundationDB.Layer.Indexer.Default.indexkey_encoder("an-example-key", 1, [])
    <<102>>

  """
  def indexkey_encoder(x, num_bytes, index_options) do
    case Keyword.get(index_options, :indexer, nil) do
      :timeseries ->
        x
        |> NaiveDateTime.add(0, :microsecond)
        |> NaiveDateTime.to_iso8601(:basic)

      nil ->
        <<n::unsigned-big-integer-size(num_bytes * 8)>> =
          <<-1::unsigned-big-integer-size(num_bytes * 8)>>

        i = :erlang.phash2(x, n)
        <<i::unsigned-big-integer-size(num_bytes * 8)>>
    end
  end

  @impl true
  def create(tx, idx) do
    index_name = idx[:id]
    source = idx[:source]
    index_fields = idx[:fields]
    options = idx[:options]

    {key_start, key_end} = Pack.primary_range(source)

    # Write the actual index for any existing data in this tenant
    #
    # If this is a large amount of data, then this transaction will surpass the 5
    # second limit.
    tx
    |> :erlfdb.get_range(key_start, key_end)
    |> :erlfdb.wait()
    |> Enum.each(fn {fdb_key, fdb_value} ->
      {index_key, index_object} =
        get_index_entry(
          {fdb_key, Pack.from_fdb_value(fdb_value)},
          index_fields,
          options,
          index_name,
          source
        )

      :erlfdb.set(tx, index_key, index_object)
    end)

    :ok
  end

  @impl true
  def set(tx, idx, kv) do
    index_name = idx[:id]
    index_fields = idx[:fields]
    index_options = idx[:options]

    {index_key, index_object} =
      get_index_entry(
        kv,
        index_fields,
        index_options,
        index_name,
        idx[:source]
      )

    :erlfdb.set(tx, index_key, index_object)
    :ok
  end

  @impl true
  def clear(tx, idx, kv) do
    index_name = idx[:id]
    index_fields = idx[:fields]
    index_options = idx[:options]

    {index_key, _index_object} =
      get_index_entry(
        kv,
        index_fields,
        index_options,
        index_name,
        idx[:source]
      )

    :erlfdb.clear(tx, index_key)
    :ok
  end

  @impl true
  def range(_idx, %QueryPlan.None{}, _options) do
    raise Unsupported, """
    FoundationDB Adapter does not support empty where clause on an index. In fact, this code path should not be reachable.
    """
  end

  def range(idx, plan = %QueryPlan.Equal{}, options) do
    index_values = for val <- [plan.param], do: indexkey_encoder(val, idx[:options])
    {start_key, end_key} = Pack.default_index_range(plan.source, idx[:id], index_values)

    start_key = options[:start_key] || start_key

    {start_key, end_key, Pack.primary_mapper()}
  end

  def range(idx, plan = %QueryPlan.Between{}, options) do
    index_options = idx[:options]

    case Keyword.get(index_options, :indexer, nil) do
      :timeseries ->
        [start_key, end_key] =
          for x <- [plan.param_left, plan.param_right],
              do: Pack.default_index_pack(plan.source, idx[:id], ["#{x}"], nil)

        start_key = options[:start_key] || start_key

        {start_key, end_key}

      nil ->
        raise Unsupported, """
        FoundationDB Adapter does not support 'between' queries on indexes that are not timeseries.
        """
    end
  end

  @impl true
  def unpack(idx, plan, fdb_kv = {_k, _v}) do
    kv = Indexer._unpack(idx, plan, fdb_kv)
    if include?(kv, plan), do: kv, else: nil
  end

  def unpack(idx, plan, {{_pkey, _pvalue}, {_skeybegin, _skeyend}, [fdb_kv]}) do
    unpack(idx, plan, fdb_kv)
  end

  defp include?(_kv, %QueryPlan.None{}) do
    true
  end

  defp include?({_key, data_object}, plan = %QueryPlan.Equal{layer_data: %{idx: _idx}}) do
    # Filter by the where_values because our indexes can have key conflicts
    plan.param == data_object[plan.field]
  end

  defp include?(_kv, %QueryPlan.Between{layer_data: %{idx: _idx}}) do
    # Between on non-timeseries index is not supported, and between on
    # time series index will not have conflicts, so no filtering needed
    true
  end

  defp include?(_kv, _plan) do
    true
  end

  # Note: pk is always first. See insert and update paths
  defp get_index_entry(
         {fdb_key, data_object = [{pk_field, pk_value} | _]},
         index_fields,
         index_options,
         index_name,
         source
       ) do
    index_fields = index_fields -- [pk_field]

    index_values =
      for idx_field <- index_fields do
        indexkey_encoder(Keyword.get(data_object, idx_field), index_options)
      end

    index_key = Pack.default_index_pack(source, index_name, index_values, pk_value)

    case Keyword.get(index_options, :indexer, nil) do
      :timeseries ->
        {index_key, Pack.to_fdb_value(data_object)}

      _ ->
        {index_key, fdb_key}
    end
  end
end
