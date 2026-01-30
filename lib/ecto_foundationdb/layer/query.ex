defmodule EctoFoundationDB.Layer.Query do
  @moduledoc false

  alias EctoFoundationDB.Exception.Unsupported
  alias EctoFoundationDB.Indexer
  alias EctoFoundationDB.Layer.Fields
  alias EctoFoundationDB.Layer.Metadata
  alias EctoFoundationDB.Layer.Pack
  alias EctoFoundationDB.Layer.PrimaryKVCodec
  alias EctoFoundationDB.Layer.Tx
  alias EctoFoundationDB.QueryPlan
  alias EctoFoundationDB.Schema

  @doc """
  Executes a query for retrieving data.

  Must be called while inside a transaction.
  """
  def all(tenant, adapter_meta, plan, options) do
    {plan, iterator} =
      Metadata.transactional(tenant, adapter_meta, plan.source, fn tx, metadata ->
        plan = make_range(metadata, plan, options)
        iterator = tx_range_iterator(tx, plan, options)
        {plan, iterator}
      end)

    FDB.LazyRangeIterator.then(iterator, &unpack_and_filter/2, %{cont_state: nil, plan: plan})
  end

  @doc """
  Executes a query for updating data.
  """
  def update(tenant, adapter_meta, plan, options) do
    Metadata.transactional(tenant, adapter_meta, plan.source, fn tx, metadata ->
      plan = make_range(metadata, plan, [])
      tx_update_range(tx, plan, metadata, options)
    end)
  end

  @doc """
  Executes a query for deleting data.
  """
  def delete(
        tenant,
        adapter_meta,
        plan = %QueryPlan{tenant: tenant, constraints: [%QueryPlan.None{}]}
      ) do
    # Special case, very efficient
    Tx.transactional(tenant, &Tx.clear_all(tenant, &1, adapter_meta, plan.source))
  end

  def delete(tenant, adapter_meta, plan) do
    Metadata.transactional(tenant, adapter_meta, plan.source, fn tx, metadata ->
      plan = make_range(metadata, plan, [])
      tx_delete_range(tx, plan, metadata)
    end)
  end

  defp make_range(
         _metadata,
         plan = %QueryPlan{constraints: [%{is_pk?: true}]},
         options
       ) do
    # @todo: getting the metadata for this request was wasteful
    make_datakey_range(plan, options)
  end

  defp make_range(
         metadata,
         plan = %QueryPlan{constraints: constraints, layer_data: layer_data},
         options
       ) do
    case Metadata.select_index(with_queryable_indexes(metadata), constraints) do
      nil ->
        raise Unsupported,
              """
              FoundationDB Adapter supports either a where clause that constrains on the primary key
              or a where clause that constrains on a set of fields that is associated with an index.
              """

      idx ->
        constraints = Metadata.arrange_constraints(constraints, idx)
        plan = %QueryPlan{plan | constraints: constraints}
        range = Indexer.range(idx, plan, options)

        layer_data =
          layer_data
          |> Map.put(:idx, idx)
          |> Map.put(:range, range)

        %QueryPlan{plan | layer_data: layer_data}
    end
  end

  defp tx_range_iterator(
         tx,
         plan = %QueryPlan{layer_data: %{range: {start_key, end_key}}},
         options
       ) do
    backward? = backward?(plan, options)

    get_options =
      options
      |> kw_take_as(:key_limit, :limit)
      |> Keyword.put(:reverse, backward?)

    FDB.LazyRangeIterator.start(tx, start_key, end_key, get_options)
  end

  defp tx_range_iterator(
         tx,
         plan = %QueryPlan{layer_data: %{range: {start_key, end_key, mapper}}},
         options
       ) do
    backward? = backward?(plan, options)

    get_options =
      options
      |> kw_take_as(:key_limit, :limit)
      |> Keyword.put(:reverse, backward?)
      |> Keyword.put(:mapper, :erlfdb_tuple.pack(mapper))

    FDB.LazyRangeIterator.start(tx, start_key, end_key, get_options)
  end

  defp tx_update_range(
         tx,
         plan = %QueryPlan{updates: updates},
         metadata,
         options
       ) do
    pk_field = Fields.get_pk_field!(plan.schema)
    write_primary = Schema.get_option(plan.context, :write_primary)

    tx
    |> tx_range_iterator(plan, [])
    |> FDB.LazyRangeIterator.then(&unpack_and_filter/2, %{cont_state: nil, plan: plan})
    |> FDB.Stream.from_iterator()
    |> Stream.map(
      &Tx.update_data_object(
        plan.tenant,
        tx,
        plan.schema,
        pk_field,
        {&1, updates},
        metadata,
        write_primary,
        options
      )
    )
    |> Enum.to_list()
    |> length()
  end

  defp tx_delete_range(tx, plan, metadata) do
    tx
    |> tx_range_iterator(plan, [])
    |> FDB.LazyRangeIterator.then(&unpack_and_filter/2, %{cont_state: nil, plan: plan})
    |> FDB.Stream.from_iterator()
    |> Stream.map(&Tx.delete_data_object(plan.tenant, tx, plan.schema, &1, metadata))
    |> Enum.to_list()
    |> length()
  end

  defp unpack_and_filter(_, state = %{plan: %QueryPlan{limit: 0}}) do
    {:halt, state}
  end

  defp unpack_and_filter([kvs], state = %{plan: plan = %QueryPlan{layer_data: %{idx: idx}}}) do
    %{limit: limit} = plan

    stream =
      kvs
      |> Stream.map(&Indexer.unpack(idx, plan, &1))
      |> Stream.filter(fn
        nil -> false
        _ -> true
      end)

    stream =
      if is_nil(limit) do
        stream
      else
        Stream.take(stream, limit)
      end

    objs = Enum.to_list(stream)

    {:cont, objs, %{state | plan: decr_limit(plan, length(objs))}}
  end

  defp unpack_and_filter([kvs], state = %{plan: plan = %QueryPlan{}}) do
    %{cont_state: cont_state} = state
    %{tenant: tenant, limit: limit} = plan
    # @todo reverse
    iterator = PrimaryKVCodec.decode_as_iterator(cont_state, kvs, tenant, limit: limit)
    {objs, iterator} = :erlfdb_iterator.run(iterator)
    cont_state = PrimaryKVCodec.get_iterator_state(iterator)
    {:cont, objs, %{state | cont_state: cont_state, plan: decr_limit(plan, length(objs))}}
  end

  defp decr_limit(plan = %QueryPlan{limit: nil}, _by), do: plan
  defp decr_limit(_plan = %QueryPlan{limit: 0}, _by), do: raise("limit bug")
  defp decr_limit(_plan = %QueryPlan{limit: limit}, by) when by > limit, do: raise("limit bug")
  defp decr_limit(plan = %QueryPlan{limit: limit}, by), do: %{plan | limit: limit - by}

  # Selects all data from source
  defp make_datakey_range(
         plan = %QueryPlan{constraints: [%QueryPlan.None{}], layer_data: layer_data},
         options
       ) do
    {start_key, end_key} = Pack.primary_range(plan.tenant, plan.source)
    start_key = options[:start_key] || start_key
    %QueryPlan{plan | layer_data: Map.put(layer_data, :range, {start_key, end_key})}
  end

  defp make_datakey_range(
         plan = %QueryPlan{
           tenant: tenant,
           constraints: [%QueryPlan.Equal{param: param}],
           layer_data: layer_data
         },
         _options
       ) do
    kv_codec = Pack.primary_codec(tenant, plan.source, param)
    %QueryPlan{plan | layer_data: Map.put(layer_data, :range, PrimaryKVCodec.range(kv_codec))}
  end

  defp make_datakey_range(
         plan = %QueryPlan{constraints: [between = %QueryPlan.Between{}]},
         options
       ) do
    %QueryPlan{tenant: tenant, layer_data: layer_data} = plan

    %QueryPlan.Between{
      param_left: param_left,
      inclusive_left?: inclusive_left?,
      param_right: param_right,
      inclusive_right?: inclusive_right?
    } = between

    {left_range_start, left_range_end} =
      if is_nil(param_left) do
        Pack.primary_range(tenant, plan.source)
      else
        codec_left = Pack.primary_codec(tenant, plan.source, param_left)
        PrimaryKVCodec.range(codec_left)
      end

    {right_range_start, right_range_end} =
      if is_nil(param_right) do
        Pack.primary_range(tenant, plan.source)
      else
        codec_right = Pack.primary_codec(tenant, plan.source, param_right)
        PrimaryKVCodec.range(codec_right)
      end

    start_key = if inclusive_left?, do: left_range_start, else: left_range_end
    end_key = if inclusive_right?, do: right_range_end, else: right_range_start

    start_key = options[:start_key] || start_key
    %QueryPlan{plan | layer_data: Map.put(layer_data, :range, {start_key, end_key})}
  end

  defp make_datakey_range(_plan, _options) do
    raise Unsupported, "Between query must have binary parameters"
  end

  defp backward?(plan = %{layer_data: %{idx: idx}}, _options) do
    %{ordering: ordering} = plan
    idx_fields = idx[:fields]
    idx_backward?(idx_fields, ordering)
  end

  defp backward?(plan, _options) do
    case {plan.schema, plan.ordering} do
      {nil, []} ->
        false

      {nil, [_ | _]} ->
        raise Unsupported, """
        Cannot apply key_limit on query ordering when schema is unknown
        """

      {schema, ordering} ->
        pk_field = Fields.get_pk_field!(schema)
        idx_backward?([pk_field], ordering)
    end
  end

  defp idx_backward?([field | _], [{:desc, field}]), do: true

  defp idx_backward?([field | fields], [{:asc, field} | ordering]),
    do: idx_backward?(fields, ordering)

  defp idx_backward?([_field | fields], ordering), do: idx_backward?(fields, ordering)

  defp idx_backward?(_, [{_, _field} | _]) do
    raise(Unsupported, """
    When querying with an order_by, the ordering must correspond to the primary key or an indexed field.
    """)
  end

  defp idx_backward?(_, _), do: false

  defp with_queryable_indexes(md = %Metadata{indexes: indexes}) do
    # @todo: custom indexes may wish to be unqueryable as well. Right now we don't expose
    # a way to exclude them.
    indexes =
      Enum.filter(indexes, fn index ->
        index[:indexer] != EctoFoundationDB.Indexer.SchemaMetadata
      end)

    %{md | indexes: indexes}
  end

  defp kw_take_as(options, from_key, to_key) do
    case Keyword.fetch(options, from_key) do
      {:ok, val} ->
        [{to_key, val}]

      :error ->
        []
    end
  end
end
