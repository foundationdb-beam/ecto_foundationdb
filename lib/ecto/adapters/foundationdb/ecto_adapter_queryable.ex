defmodule Ecto.Adapters.FoundationDB.EctoAdapterQueryable do
  @moduledoc false
  @behaviour Ecto.Adapter.Queryable

  alias Ecto.Adapters.FoundationDB
  alias EctoFoundationDB.Exception.IncorrectTenancy
  alias EctoFoundationDB.Future
  alias EctoFoundationDB.Layer.Fields
  alias EctoFoundationDB.Layer.Ordering
  alias EctoFoundationDB.Layer.Query
  alias EctoFoundationDB.Layer.Tx
  alias EctoFoundationDB.QueryPlan
  alias EctoFoundationDB.Schema
  alias EctoFoundationDB.Tenant

  @impl Ecto.Adapter.Queryable
  def prepare(
        operation,
        query = %Ecto.Query{
          order_bys: order_bys,
          limit: limit
        }
      ) do
    ordering_fn = Ordering.get_ordering_fn(order_bys)
    limit = get_limit(limit)
    limit_fn = if limit == nil, do: & &1, else: &Stream.take(&1, limit)
    {:nocache, {operation, query, {limit, limit_fn}, %{}, ordering_fn}}
  end

  @impl Ecto.Adapter.Queryable
  def execute(
        adapter_meta = %{opts: adapter_opts},
        _query_meta,
        _query_cache =
          {:nocache,
           {:all,
            query = %Ecto.Query{
              from: %Ecto.Query.FromExpr{source: {_source, schema}},
              select: %Ecto.Query.SelectExpr{
                fields: select_fields
              }
            }, {_limit, limit_fn}, %{}, ordering_fn}},
        params,
        options
      ) do
    case options[:noop] do
      query_result when not is_nil(query_result) ->
        # This is the trick to load structs after a pipelined 'get'. See async_get_by, await, etc
        query_result

      _ ->
        {context, query = %Ecto.Query{prefix: tenant}} = assert_tenancy!(query, adapter_opts)

        future = execute_all(tenant, adapter_meta, context, query, params)

        future =
          Future.apply(future, fn {objs, _continuation} ->
            objs
            |> ordering_fn.()
            |> limit_fn.()
            |> select(Fields.parse_select_fields(select_fields), nil)
          end)

        handle_returning(schema, future, options)
    end
  end

  def execute(
        adapter_meta = %{opts: adapter_opts},
        _query_meta,
        _query_cache =
          {:nocache,
           {:delete_all,
            query = %Ecto.Query{
              from: %Ecto.Query.FromExpr{source: {source, schema}},
              wheres: wheres
            }, {nil, _limit_fn}, %{}, _ordering_fn}},
        params,
        _options
      ) do
    {context, %Ecto.Query{prefix: tenant}} = assert_tenancy!(query, adapter_opts)

    plan = QueryPlan.get(tenant, source, schema, context, wheres, [], params)
    num = Query.delete(tenant, adapter_meta, plan)

    {num, []}
  end

  def execute(
        adapter_meta = %{opts: adapter_opts},
        _query_meta,
        _query_cache =
          {:nocache, {:update_all, query, {nil, _limit_fn}, %{}, _ordering_fn}},
        params,
        _options
      ) do
    {context, query = %Ecto.Query{prefix: tenant}} = assert_tenancy!(query, adapter_opts)

    num =
      execute_update_all(tenant, adapter_meta, context, query, params)

    {num, []}
  end

  @impl Ecto.Adapter.Queryable
  def stream(
        adapter_meta = %{opts: adapter_opts},
        _query_meta,
        _query_cache =
          {:nocache, {:all, query, {nil, _limit_fn}, %{}, _ordering_fn}},
        params,
        options
      ) do
    {context, query = %Ecto.Query{prefix: tenant}} = assert_tenancy!(query, adapter_opts)

    tenant
    |> stream_all(adapter_meta, context, query, params, options)
  end

  # Extract limit from an `Ecto.Query`
  defp get_limit(nil), do: nil
  defp get_limit(%Ecto.Query.QueryExpr{expr: limit}), do: limit

  defp assert_tenancy!(
         query = %Ecto.Query{
           prefix: tenant,
           from: %Ecto.Query.FromExpr{source: {source, schema}}
         },
         _adapter_opts
       ) do
    context = Schema.get_context!(source, schema)

    case Tx.safe?(tenant) do
      {false, :missing_tenant} ->
        raise IncorrectTenancy, """
        FoundationDB Adapter is expecting the query for schema \
        #{inspect(schema)} to include a tenant in the prefix metadata, \
        but a nil prefix was provided.

        Use `prefix: tenant` in your query.
        """

      {true, tenant = %Tenant{}} ->
        {context, %Ecto.Query{query | prefix: tenant}}
    end
  end

  defp execute_all(
         tenant,
         adapter_meta,
         context,
         %Ecto.Query{
           from: %Ecto.Query.FromExpr{source: {source, schema}},
           wheres: wheres
         },
         params
       ) do
    # Steps:
    #   0. Validate wheres for supported query types
    #     i. Equal -> where_field == param[0]
    #     ii. Between -> where_field > param[0] and where_field < param[1]
    #     iii. None -> empty where clause
    #   1. pk or index?
    #   2. construct start key and end key from the first where expression
    #   3. Use :erlfdb.get, :erlfdb.get_range
    #   4. Post-get filtering (Remove :not_found, remove index conflicts, )
    #   5. Arrange fields based on the select input
    plan = QueryPlan.get(tenant, source, schema, context, wheres, [], params)

    Query.all(tenant, adapter_meta, plan)
  end

  defp handle_returning(schema, future, options) do
    case options[:returning] do
      {:future, all_or_one} ->
        Process.put(
          Future.token(),
          {schema, Future.apply(future, fn res -> {all_or_one, res} end)}
        )

        {0, []}

      _ ->
        # Future: If there is a wrapping transaction without an `async_*` qualifier, the wait happens here
        Future.result(future)
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
    plan = QueryPlan.get(tenant, source, schema, context, wheres, updates, params)
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

    query_options = fn
      nil ->
        [limit: fdb_limit]

      %Query.Continuation{start_key: start_key} ->
        [start_key: start_key, limit: fdb_limit]

      x ->
        raise "opt #{inspect(x)}"
    end

    start_fun = fn ->
      plan = QueryPlan.get(tenant, source, schema, context, wheres, [], params)

      %{
        adapter_meta: adapter_meta,
        tenant: tenant,
        query_options: query_options,
        user_callback: options[:tx_callback],
        plan: plan,
        field_names: Fields.parse_select_fields(select_fields),
        select_fields: select_fields,
        continuation: nil
      }
    end

    Stream.resource(start_fun, &stream_all_next/1, &stream_all_after/1)
  end

  defp stream_all_next(acc = %{continuation: %Query.Continuation{more?: false}}) do
    {:halt, acc}
  end

  defp stream_all_next(acc) do
    %{
      tenant: tenant,
      adapter_meta: adapter_meta,
      query_options: query_options,
      field_names: field_names,
      user_callback: user_callback,
      plan: plan,
      continuation: continuation
    } = acc

    # Note for future: wrapping Query.all in a transaction is tricky because it creates the
    # future internally, which doesn't allow us to use the before_transactional test.
    # To work around this, we make sure we resolve the future inside the transactional.
    # It doesn't matter because the 'stream' API doesn't support async_* APIs.
    FoundationDB.transactional(tenant, fn tx ->
      future = Query.all(tenant, adapter_meta, plan, query_options.(continuation))

      tx_callback = if is_nil(user_callback), do: nil, else: &user_callback.(tx, &1)

      future =
        Future.apply(future, fn {objs, continuation} ->
          {[select(objs, field_names, tx_callback)], %{acc | continuation: continuation}}
        end)

      Future.result(future)
    end)
  end

  defp stream_all_after(_acc), do: :ok

  defp select(objs, select_field_names, callback) do
    stream = if is_nil(callback), do: objs, else: callback.(objs)

    rows =
      stream
      |> Stream.map(fn data_object ->
        Fields.arrange(data_object, select_field_names)
      end)
      |> Fields.strip_field_names_for_ecto()
      |> Enum.to_list()

    {length(rows), rows}
  end
end
