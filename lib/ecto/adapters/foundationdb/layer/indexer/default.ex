defmodule Ecto.Adapters.FoundationDB.Layer.Indexer.Default do
  @moduledoc false
  alias Ecto.Adapters.FoundationDB.Exception.Unsupported
  alias Ecto.Adapters.FoundationDB.Layer.Indexer
  alias Ecto.Adapters.FoundationDB.Layer.Pack
  alias Ecto.Adapters.FoundationDB.Options
  alias Ecto.Adapters.FoundationDB.QueryPlan

  @behaviour Indexer

  @index_namespace "i"

  @doc """
  In the index key, values must be encoded into a fixed-length binary.

  Fixed-length is required so that get_range can be used reliably in the presence of
  arbitrary data. In a naive approach, the key_delimiter can conflict with
  the bytes included in the index value.

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
  def create(tx, idx, %{opts: adapter_opts}) do
    index_name = idx[:id]
    source = idx[:source]
    index_fields = idx[:fields]
    options = idx[:options]

    key_startswith = Pack.to_fdb_datakey_startswith(adapter_opts, source)
    key_start = key_startswith
    key_end = :erlfdb_key.strinc(key_startswith)

    # Prevent updates on the keys so that we write the correct index values
    :erlfdb.add_write_conflict_range(tx, key_start, key_end)

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
          adapter_opts,
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
  def set(tx, idx, adapter_meta, kv) do
    index_name = idx[:id]
    index_fields = idx[:fields]
    index_options = idx[:options]

    {index_key, index_object} =
      get_index_entry(
        Map.get(adapter_meta, :opts, %{}),
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
  def clear(tx, idx, adapter_meta, kv) do
    index_name = idx[:id]
    index_fields = idx[:fields]
    index_options = idx[:options]

    {index_key, _index_object} =
      get_index_entry(
        Map.get(adapter_meta, :opts, %{}),
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
  def range(_idx, _adapter_meta, %QueryPlan.None{}, _options) do
    raise Unsupported, """
    FoundationDB Adapter does not support empty where clause on an index. In fact, this code path should not be reachable.
    """
  end

  def range(idx, %{opts: adapter_opts}, plan = %QueryPlan.Equal{}, options) do
    start_key =
      to_fdb_indexkey(
        adapter_opts,
        idx[:options],
        plan.source,
        idx[:id],
        [plan.param],
        nil
      )

    end_key = :erlfdb_key.strinc(start_key)
    start_key = options[:start_key] || start_key

    {start_key, end_key}
  end

  def range(idx, %{opts: adapter_opts}, plan = %QueryPlan.Between{}, options) do
    index_options = idx[:options]

    case Keyword.get(index_options, :indexer, nil) do
      :timeseries ->
        [start_key, end_key] =
          for x <- [plan.param_left, plan.param_right],
              do: to_fdb_indexkey(adapter_opts, idx[:options], plan.source, idx[:id], [x], nil)

        start_key = options[:start_key] || start_key

        {start_key, end_key}

      nil ->
        raise Unsupported, """
        FoundationDB Adapter does not support 'between' queries on indexes that are not timeseries.
        """
    end
  end

  @impl true
  def unpack(idx, plan, fdb_kv) do
    kv = Indexer._unpack(idx, plan, fdb_kv)
    if include?(kv, plan), do: kv, else: nil
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
         adapter_opts,
         {fdb_key, data_object = [{pk_field, pk_value} | _]},
         index_fields,
         index_options,
         index_name,
         source
       ) do
    index_fields = index_fields -- [pk_field]

    index_entries =
      for idx_field <- index_fields, do: {idx_field, Keyword.get(data_object, idx_field)}

    {_, path_vals} = Enum.unzip(index_entries)

    index_key =
      to_fdb_indexkey(
        adapter_opts,
        index_options,
        source,
        "#{index_name}",
        path_vals,
        pk_value
      )

    {index_key, Indexer.pack({fdb_key, data_object})}
  end

  #  iex> Ecto.Adapters.FoundationDB.Layer.Indexer.Default.to_fdb_indexkey([], [],
  #  ...> "table", "my_index", ["abc", "123"], "my-pk-id")
  #  "\\xFEtable/i/my_index/M\\xDA\\xD8\\xFB/V\\xE3\\xD1\\x01/\\x83m\\0\\0\\0\\bmy-pk-id"
  defp to_fdb_indexkey(adapter_opts, index_options, source, index_name, vals, id)
       when is_list(vals) do
    fun = Options.get(adapter_opts, :indexkey_encoder)
    vals = for v <- vals, do: fun.(v, index_options)

    Pack.to_raw_fdb_key(
      adapter_opts,
      [source, @index_namespace, index_name | vals] ++
        if(is_nil(id), do: [], else: [Pack.encode_pk_for_key(id)])
    )
  end
end
