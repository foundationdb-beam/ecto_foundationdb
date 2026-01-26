defmodule Ecto.Adapters.FoundationDB.EctoAdapterQueryable do
  @moduledoc false
  @behaviour Ecto.Adapter.Queryable

  alias Ecto.Adapters.FoundationDB.EctoAdapterQueryable.Continuation

  alias EctoFoundationDB.Assert.CorrectTenancy
  alias EctoFoundationDB.Exception.Unsupported
  alias EctoFoundationDB.Future
  alias EctoFoundationDB.Layer.Fields
  alias EctoFoundationDB.Layer.Ordering
  alias EctoFoundationDB.Layer.Query
  alias EctoFoundationDB.Layer.Tx
  alias EctoFoundationDB.QueryPlan
  alias EctoFoundationDB.Schema

  alias FDB.BarrierStream

  defmodule Continuation do
    @moduledoc false
    defstruct more?: false, start_key: nil
  end

  @impl Ecto.Adapter.Queryable
  def prepare(operation, query) do
    %Ecto.Query{
      from: %Ecto.Query.FromExpr{source: {_source, _schema}},
      order_bys: order_bys,
      limit: limit
    } = query

    ordering = Ordering.get_ordering(order_bys)
    limit = get_query_limit(limit)
    limit_fn = if limit == nil, do: & &1, else: &Stream.take(&1, limit)
    {:nocache, {operation, query, {limit, limit_fn}, %{}, ordering}}
  end

  @impl Ecto.Adapter.Queryable
  def execute(
        adapter_meta = %{opts: repo_config},
        _query_meta,
        _query_cache =
          {:nocache,
           {:all,
            query = %Ecto.Query{
              from: %Ecto.Query.FromExpr{source: {source, schema}},
              wheres: wheres,
              select: %Ecto.Query.SelectExpr{
                fields: select_fields
              }
            }, {limit, limit_fn}, %{}, ordering}},
        params,
        options
      ) do
    options = adjust_limit_options(options, limit)

    case options[:noop] do
      query_result when not is_nil(query_result) ->
        # This is the trick to load structs after a pipelined 'get'. See async_get_by, await, etc
        query_result

      _ ->
        {%{context: context}, %Ecto.Query{prefix: tenant}} =
          CorrectTenancy.assert_by_query!(repo_config, query)

        plan = QueryPlan.get(tenant, source, schema, context, wheres, [], params, ordering)

        must_wait? = not Tx.in_tx?()

        Tx.transactional(tenant, fn _tx ->
          barrier_stream = Query.all(tenant, adapter_meta, plan, options)

          if must_wait? do
            {_, barrier_stream} = BarrierStream.advance(barrier_stream)
            barrier_stream
          else
            barrier_stream
          end
        end)
        |> BarrierStream.to_stream()
        |> limit_fn.()
        |> select(Fields.parse_select_fields(select_fields))
        |> then(&Future.new(:stream, &1))
        |> handle_returning(options)
    end
  end

  def execute(
        adapter_meta = %{opts: repo_config},
        _query_meta,
        _query_cache =
          {:nocache,
           {:delete_all,
            query = %Ecto.Query{
              from: %Ecto.Query.FromExpr{source: {source, schema}},
              wheres: wheres
            }, {nil, _limit_fn}, %{}, _ordering}},
        params,
        _options
      ) do
    {%{context: context}, %Ecto.Query{prefix: tenant}} =
      CorrectTenancy.assert_by_query!(repo_config, query)

    plan = QueryPlan.get(tenant, source, schema, context, wheres, [], params, [])
    num = Query.delete(tenant, adapter_meta, plan)

    {num, []}
  end

  def execute(
        adapter_meta = %{opts: repo_config},
        _query_meta,
        _query_cache =
          {:nocache, {:update_all, query, {nil, _limit_fn}, %{}, _ordering}},
        params,
        _options
      ) do
    {%{context: context}, query = %Ecto.Query{prefix: tenant}} =
      CorrectTenancy.assert_by_query!(repo_config, query)

    num =
      execute_update_all(tenant, adapter_meta, context, query, params)

    {num, []}
  end

  @impl Ecto.Adapter.Queryable
  def stream(
        adapter_meta = %{opts: repo_config},
        _query_meta,
        _query_cache =
          {:nocache, {:all, query, {nil, _limit_fn}, %{}, []}},
        params,
        options
      ) do
    {%{context: context}, query = %Ecto.Query{prefix: tenant}} =
      CorrectTenancy.assert_by_query!(repo_config, query)

    tenant
    |> stream_all(adapter_meta, context, query, params, options)
  end

  def stream(
        _adapter_meta,
        _query_meta,
        _query_cache =
          {:nocache, {:all, _query, {nil, _limit_fn}, %{}, _ordering}},
        _params,
        _options
      ) do
    raise Unsupported, """
    Stream ordering is not supported.
    """
  end

  def execute_all_range(_module, _repo, queryable, id_s, id_e, {adapter_meta, options}) do
    %{opts: repo_config} = adapter_meta

    {schema, source} = queryable_to_schema_source_tuplet(queryable)

    {select_fields, return_handler} =
      queryable_to_select_fields_return_handler_tuplet(queryable, schema)

    %{context: context, prefix: tenant} =
      CorrectTenancy.assert_by_schema!(repo_config, %{
        prefix: queryable_to_tenant(queryable, options),
        source: source,
        schema: schema
      })

    plan = QueryPlan.all_range(tenant, source, schema, context, id_s, id_e, options)

    must_wait? = not Tx.in_tx?()

    {_, barrier_stream} =
      Tx.transactional(tenant, fn _tx ->
        barrier_stream = Query.all(tenant, adapter_meta, plan, options)
        if must_wait?, do: BarrierStream.advance(barrier_stream), else: barrier_stream
      end)

    barrier_stream
    |> BarrierStream.to_stream()
    |> then(&Future.new(:stream, &1))
    |> Future.then(fn objs ->
      case return_handler do
        :all_from_source ->
          objs = Enum.to_list(objs)
          select_fields = select_fields || get_field_names_union(objs)
          {select_fields, select(objs, select_fields)}

        _ ->
          select(objs, select_fields)
      end
    end)
    |> handle_returning(options ++ [returning: {:future, return_handler}])
  end

  # Extract limit from an `Ecto.Query`
  defp get_query_limit(nil), do: nil
  defp get_query_limit(%Ecto.Query.LimitExpr{expr: limit}), do: limit

  defp adjust_limit_options(options, nil), do: options

  defp adjust_limit_options(options, query_limit) do
    {_, options} =
      Keyword.get_and_update(options, :key_limit, fn
        nil ->
          {nil, query_limit}

        option_limit ->
          {option_limit, option_limit}
      end)

    options
  end

  defp handle_returning(future, options) do
    case options[:returning] do
      {:future, return_handler} ->
        future = Future.then(future, fn enum -> {return_handler, enum} end)
        Process.put(Future.token(), future)
        {0, []}

      _ ->
        rows = Future.result(future)
        {length(rows), rows}
    end
  end

  defp execute_update_all(
         tenant,
         adapter_meta = %{opts: options},
         context,
         %Ecto.Query{
           from: %Ecto.Query.FromExpr{source: {source, schema}},
           wheres: wheres,
           updates: updates
         },
         params
       ) do
    plan = QueryPlan.get(tenant, source, schema, context, wheres, updates, params, [])
    Query.update(tenant, adapter_meta, plan, options)
  end

  defp stream_all(tenant, adapter_meta, context, query, params, options) do
    %Ecto.Query{
      select: %Ecto.Query.SelectExpr{
        fields: select_fields
      },
      from: %Ecto.Query.FromExpr{source: {source, schema}},
      wheres: wheres
    } = query

    # :max_rows - The number of rows to load from the database as we stream.
    # It is supported at least by Postgres and MySQL and defaults to 500.
    fdb_limit = options[:max_rows] || 500

    ef_query_options = fn
      nil ->
        [key_limit: fdb_limit]

      %Continuation{start_key: start_key} ->
        [start_key: start_key, key_limit: fdb_limit]

      x ->
        raise "opt #{inspect(x)}"
    end

    start_fun = fn ->
      plan = QueryPlan.get(tenant, source, schema, context, wheres, [], params, [])

      %{
        adapter_meta: adapter_meta,
        tenant: tenant,
        ef_query_options: ef_query_options,
        user_callback: options[:tx_callback],
        plan: plan,
        field_names: Fields.parse_select_fields(select_fields),
        select_fields: select_fields,
        continuation: nil
      }
    end

    Stream.resource(start_fun, &stream_all_next/1, &stream_all_after/1)
  end

  defp stream_all_next(acc = %{continuation: %Continuation{more?: false}}) do
    {:halt, acc}
  end

  defp stream_all_next(acc) do
    %{
      tenant: tenant,
      adapter_meta: adapter_meta,
      ef_query_options: ef_query_options,
      field_names: field_names,
      user_callback: user_callback,
      plan: plan,
      continuation: continuation
    } = acc

    options = ef_query_options.(continuation)

    {cont, barrier_stream} =
      Tx.transactional(tenant, fn tx ->
        # Advance the stream so that we can retrieve the last key for building
        # the continuation
        {cont, barrier_stream} =
          Query.all(tenant, adapter_meta, plan, options)
          |> BarrierStream.advance(fn kvs -> {length(kvs), List.last(kvs)} end)

        barrier_stream =
          if is_nil(user_callback) do
            barrier_stream
          else
            barrier_stream
            |> BarrierStream.then(&user_callback.(tx, &1))
            |> BarrierStream.set_barrier()
          end

        {_, barrier_stream} = BarrierStream.advance(barrier_stream)
        {cont, barrier_stream}
      end)

    objs =
      barrier_stream
      |> BarrierStream.to_stream()
      |> select(field_names)
      |> Enum.to_list()

    {[objs], %{acc | continuation: continuation(cont, options)}}
  end

  defp stream_all_after(_acc), do: :ok

  defp get_field_names_union(objs) do
    {all_fields, _} =
      objs
      |> Enum.reduce({[], MapSet.new()}, fn data_object, {list, set_a} ->
        fields = Keyword.keys(data_object)
        set_b = MapSet.new(fields)

        if MapSet.size(set_a) == 0 do
          {fields, set_b}
        else
          new_set = MapSet.union(set_a, set_b)
          new_fields = MapSet.difference(set_a, set_b) |> MapSet.to_list()
          {list ++ new_fields, new_set}
        end
      end)

    all_fields
  end

  defp select(stream, select_field_names) do
    stream
    |> Stream.map(fn data_object ->
      Fields.arrange(data_object, select_field_names)
    end)
    |> Fields.strip_field_names_for_ecto()
  end

  defp queryable_to_schema_source_tuplet(queryable) do
    cond do
      is_atom(queryable) ->
        {queryable, Schema.get_source(queryable)}

      is_binary(queryable) ->
        {nil, queryable}

      is_struct(queryable, Ecto.Query) ->
        %Ecto.Query{from: %Ecto.Query.FromExpr{source: {source, schema}}} = queryable
        {schema, source}
    end
  end

  defp queryable_to_select_fields_return_handler_tuplet(queryable, schema) do
    case queryable do
      %Ecto.Query{
        from: %Ecto.Query.FromExpr{source: {_source, schema}},
        select: %Ecto.Query.SelectExpr{
          fields: select_fields
        }
      }
      when is_list(select_fields) and select_fields !== [] ->
        return_handler = if schema, do: :all, else: :all_from_source
        {Fields.parse_select_fields(select_fields), return_handler}

      %Ecto.Query{
        from: %Ecto.Query.FromExpr{source: {_source, nil}},
        select: %Ecto.Query.SelectExpr{
          fields: nil,
          expr: {:&, [], [0]},
          take: %{0 => {:any, select_fields}}
        }
      }
      when is_list(select_fields) ->
        {select_fields, :all_from_source}

      _ ->
        if is_nil(schema) do
          {nil, :all_from_source}
        else
          {schema.__schema__(:fields), :all}
        end
    end
  end

  defp queryable_to_tenant(%Ecto.Query{prefix: tenant}, options) do
    options[:prefix] || tenant
  end

  defp queryable_to_tenant(_queryable, options) do
    options[:prefix]
  end

  defp continuation({len, last_kv}, options) do
    case options[:key_limit] do
      nil ->
        %Continuation{more?: false}

      key_limit ->
        if len >= key_limit do
          {fdb_key, _} = last_kv
          %Continuation{more?: true, start_key: :erlfdb_key.strinc(fdb_key)}
        else
          %Continuation{more?: false}
        end
    end
  end
end
