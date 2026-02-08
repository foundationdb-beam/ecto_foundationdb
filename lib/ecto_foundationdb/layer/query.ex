defmodule EctoFoundationDB.Layer.Query do
  @moduledoc false

  alias EctoFoundationDB.Exception.Unsupported
  alias EctoFoundationDB.Indexer
  alias EctoFoundationDB.Layer.Fields
  alias EctoFoundationDB.Layer.Metadata
  alias EctoFoundationDB.Layer.Ordering
  alias EctoFoundationDB.Layer.Pack
  alias EctoFoundationDB.Layer.PrimaryKVCodec
  alias EctoFoundationDB.Layer.Tx
  alias EctoFoundationDB.QueryPlan
  alias EctoFoundationDB.Schema

  defstruct [:idx, :range, :backward?]

  @doc """
  Executes a query for retrieving data.

  Must be called while inside a transaction.
  """
  def all(_tenant, _adapter_meta, plan = %QueryPlan{constraints: [%{pk?: true}]}, options) do
    # Single constraint on the primary key, skip the metadata retrieval
    {plan, iterator, post_query_ordering_fn} = tx_all(Tx.get(), nil, plan, options)

    {FDB.LazyRangeIterator.then(iterator, &unpack_and_filter/2, %{cont_state: nil, plan: plan}),
     post_query_ordering_fn}
  end

  def all(tenant, adapter_meta, plan = %QueryPlan{}, options) do
    assert_repo_limit_omitted(options)

    {plan, iterator, post_query_ordering_fn} =
      Metadata.transactional(tenant, adapter_meta, plan.source, fn tx, metadata ->
        tx_all(tx, metadata, plan, options)
      end)

    {FDB.LazyRangeIterator.then(iterator, &unpack_and_filter/2, %{cont_state: nil, plan: plan}),
     post_query_ordering_fn}
  end

  defp tx_all(tx, metadata, plan, options) do
    {plan, post_query_ordering_fn} = make_range(metadata, plan, options)
    iterator = tx_range_iterator(tx, plan, options)
    {plan, iterator, post_query_ordering_fn}
  end

  @doc """
  Executes a query for updating data.
  """
  def update(tenant, adapter_meta, plan, options) do
    Metadata.transactional(tenant, adapter_meta, plan.source, fn tx, metadata ->
      {plan, nil} = make_range(metadata, plan, [])
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
      {plan, nil} = make_range(metadata, plan, [])
      tx_delete_range(tx, plan, metadata)
    end)
  end

  defp make_range(
         _metadata,
         plan = %QueryPlan{constraints: [%{pk?: true}]},
         options
       ) do
    %{schema: schema, ordering: ordering, limit: limit} = plan

    {query_ordering, post_query_ordering_fn} =
      get_query_ordering(schema, nil, [], limit, ordering, options)

    plan = make_datakey_range(plan, options)
    plan = backward?(plan, query_ordering, options)

    {plan, post_query_ordering_fn}
  end

  defp make_range(
         metadata,
         plan = %QueryPlan{constraints: constraints, layer_data: layer_data = %__MODULE__{}},
         options
       )
       when not is_nil(metadata) do
    %{schema: schema, ordering: ordering, limit: limit} = plan

    case Metadata.select_index(with_queryable_indexes(metadata), constraints, ordering) do
      nil ->
        case constraints do
          [%QueryPlan.None{}] ->
            {query_ordering, post_query_ordering_fn} =
              get_query_ordering(schema, nil, [], limit, ordering, options)

            plan = make_datakey_range(plan, options)
            plan = backward?(plan, query_ordering, options)
            {plan, post_query_ordering_fn}

          _ ->
            raise Unsupported,
                  """
                  FoundationDB Adapter supports either a where clause that constrains on the primary key
                  or a where clause that constrains on a set of fields that is associated with an index.
                  """
        end

      idx ->
        {query_ordering, post_query_ordering_fn} =
          get_query_ordering(schema, idx, constraints, limit, ordering, options)

        constraints = Metadata.arrange_constraints(constraints, idx)
        plan = %QueryPlan{plan | constraints: constraints}
        range = Indexer.range(idx, plan, options)

        layer_data = %{layer_data | idx: idx, range: range}

        plan = %{plan | layer_data: layer_data}
        plan = backward?(plan, query_ordering, options)
        {plan, post_query_ordering_fn}
    end
  end

  defp tx_range_iterator(
         tx,
         %QueryPlan{layer_data: %__MODULE__{range: {start_key, end_key}, backward?: backward?}},
         options
       ) do
    get_options =
      options
      |> kw_take_as(:key_limit, :limit)
      |> Keyword.put(:reverse, backward?)

    FDB.LazyRangeIterator.start(tx, start_key, end_key, get_options)
  end

  defp tx_range_iterator(
         tx,
         %QueryPlan{
           layer_data: %__MODULE__{range: {start_key, end_key, mapper}, backward?: backward?}
         },
         options
       ) do
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

  defp unpack_and_filter(
         [kvs],
         state = %{plan: plan = %QueryPlan{layer_data: %__MODULE__{idx: idx}}}
       )
       when not is_nil(idx) do
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
    %{tenant: tenant, limit: limit, layer_data: layer_data = %__MODULE__{}} = plan
    %{backward?: backward?} = layer_data

    iterator =
      PrimaryKVCodec.decode_as_iterator(cont_state, kvs, tenant,
        backward?: backward?,
        limit: limit
      )

    {objs, iterator} = PrimaryKVCodec.Iterator.run(iterator)
    cont_state = PrimaryKVCodec.get_iterator_state(iterator)
    {:cont, objs, %{state | cont_state: cont_state, plan: decr_limit(plan, length(objs))}}
  end

  defp decr_limit(plan = %QueryPlan{limit: nil}, _by), do: plan
  defp decr_limit(_plan = %QueryPlan{limit: 0}, _by), do: raise("limit bug")
  defp decr_limit(_plan = %QueryPlan{limit: limit}, by) when by > limit, do: raise("limit bug")
  defp decr_limit(plan = %QueryPlan{limit: limit}, by), do: %{plan | limit: limit - by}

  # Selects all data from source
  defp make_datakey_range(
         plan = %QueryPlan{
           constraints: [%QueryPlan.None{}],
           layer_data: layer_data = %__MODULE__{}
         },
         options
       ) do
    {start_key, end_key} = Pack.primary_range(plan.tenant, plan.source)
    start_key = options[:start_key] || start_key
    layer_data = %{layer_data | range: {start_key, end_key}}
    %{plan | layer_data: layer_data}
  end

  defp make_datakey_range(
         plan = %QueryPlan{
           tenant: tenant,
           constraints: [%QueryPlan.Equal{param: param}],
           layer_data: layer_data = %__MODULE__{}
         },
         _options
       ) do
    kv_codec = Pack.primary_codec(tenant, plan.source, param)
    layer_data = %{layer_data | range: PrimaryKVCodec.range(kv_codec)}
    %{plan | layer_data: layer_data}
  end

  defp make_datakey_range(
         plan = %QueryPlan{constraints: [between = %QueryPlan.Between{}]},
         options
       ) do
    %{tenant: tenant, layer_data: layer_data = %__MODULE__{}} = plan

    %{
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
    layer_data = %{layer_data | range: {start_key, end_key}}
    %{plan | layer_data: layer_data}
  end

  defp make_datakey_range(_plan, _options) do
    raise Unsupported, "Between query must have binary parameters"
  end

  defp backward?(plan = %{layer_data: layer_data = %__MODULE__{idx: idx}}, ordering, _options)
       when not is_nil(idx) do
    idx_fields = idx[:fields]
    backward? = idx_backward?(idx_fields, ordering)
    layer_data = %{layer_data | backward?: backward?}
    %{plan | layer_data: layer_data}
  end

  defp backward?(plan, ordering, _options) do
    %{layer_data: layer_data = %__MODULE__{}} = plan

    backward? =
      case {plan.schema, ordering} do
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

    layer_data = %{layer_data | backward?: backward?}
    %{plan | layer_data: layer_data}
  end

  defp idx_backward?([field | _], [%QueryPlan.Order{monotonicity: :desc, field: field}]), do: true

  defp idx_backward?([field | fields], [
         %QueryPlan.Order{monotonicity: :asc, field: field} | ordering
       ]),
       do: idx_backward?(fields, ordering)

  defp idx_backward?([_field | fields], ordering), do: idx_backward?(fields, ordering)

  defp idx_backward?(_, [%QueryPlan.Order{field: field} | _]) do
    raise Unsupported, """
    When querying with a key_limit, order_by must correspond to the primary key or an indexed field.

      order_by field: #{inspect(field)}
    """
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

  defp get_query_ordering(_schema, _idx, _constraints, _limit, [], _options) do
    # When querying without any ordering, limiting will work just fine.
    # The keys are naturally ordered in the database.
    {[], nil}
  end

  defp get_query_ordering(
         schema,
         nil,
         [],
         limit,
         ordering = [qo = %QueryPlan.Order{} | tail_ordering],
         options
       ) do
    # When querying without an index, we must check the limiting characertics of the query.
    #
    # If the query includes a limit, then any ordering must be only on the primary key
    #
    # If there's no limit, then we can support post query ordering

    %{pk?: pk?} = qo

    limited? = !is_nil(limit) or Keyword.has_key?(options, :key_limit)
    pk_only_ordering? = pk? and Enum.empty?(tail_ordering)

    cond do
      limited? and pk_only_ordering? ->
        {[qo], nil}

      limited? and not pk_only_ordering? ->
        ordering_fields = for %QueryPlan.Order{field: f} <- ordering, do: f

        raise Unsupported, """
        When querying with a limit, ordering must be on the primary key only.

          order_by: #{inspect(ordering_fields)}

        To order by non-primary-key fields with a limit, create an index that
        includes those fields and add a where clause that constrains on the index.

        Without a limit, any order_by is supported (applied after fetching all results).
        """

      not limited? and pk_only_ordering? ->
        fun = Ordering.get_post_query_ordering_fn(schema, ordering)
        {[qo], fun}

      not limited? and not pk_only_ordering? ->
        fun = Ordering.get_post_query_ordering_fn(schema, ordering)
        {[], fun}
    end
  end

  defp get_query_ordering(
         schema,
         idx,
         constraints,
         limit,
         ordering = [qo = %QueryPlan.Order{} | tail_ordering],
         options
       )
       when not is_nil(idx) do
    # When querying with an index, we must check the limiting characteristics of the query.
    #
    # If the query includes a limit, then the ordering must be only the first field to appear
    # in the idx after all Equal constraints.
    #
    # If there's no limit, then we can support post query ordering
    %{field: first_ordering_field} = qo

    limited? = !is_nil(limit) or Keyword.has_key?(options, :key_limit)
    ffaec = get_first_field_after_equal_constraints(idx[:fields], constraints)
    ffaec_only_ordering? = ffaec == first_ordering_field and Enum.empty?(tail_ordering)

    cond do
      limited? and ffaec_only_ordering? ->
        {[qo], nil}

      limited? and not ffaec_only_ordering? ->
        ordering_fields = for %QueryPlan.Order{field: f} <- ordering, do: f
        equal_fields = for %QueryPlan.Equal{field: f} <- constraints, do: f

        raise Unsupported, """
        When querying with a limit on index #{inspect(idx[:id])}, ordering must be
        on the first field after your equal constraints.

          order_by: #{inspect(ordering_fields)}
          index fields: #{inspect(idx[:fields])}
          equal constraints on: #{inspect(equal_fields)}
          valid order_by field: #{inspect(ffaec)}

        Without a limit, any order_by is supported (applied after fetching all results).
        """

      not limited? and ffaec_only_ordering? ->
        fun = Ordering.get_post_query_ordering_fn(schema, ordering)
        {[qo], fun}

      not limited? and not ffaec_only_ordering? ->
        fun = Ordering.get_post_query_ordering_fn(schema, ordering)
        {[], fun}
    end
  end

  defp get_first_field_after_equal_constraints(idx_fields, constraints) do
    equal_fields = for %QueryPlan.Equal{field: field} <- constraints, do: field

    case idx_fields -- equal_fields do
      [ffaec | _] ->
        ffaec

      _ ->
        nil
    end
  end

  defp assert_repo_limit_omitted(options) do
    if Keyword.has_key?(options, :limit) do
      raise Unsupported,
            "`:limit` in Repo options is not supported. Use an `Ecto.Query` limit instead."
    end
  end
end
