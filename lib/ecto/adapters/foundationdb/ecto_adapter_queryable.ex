defmodule Ecto.Adapters.FoundationDB.EctoAdapterQueryable do
  @moduledoc false
  @behaviour Ecto.Adapter.Queryable

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
            |> select(Fields.parse_select_fields(select_fields))
          end)

        handle_returning(future, options)
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

      {true, tenant=%Tenant{}} ->
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

  defp handle_returning(future, options) do
    case options[:returning] do
      {:future, all_or_one} ->
        Process.put(Future.token(), Future.apply(future, fn res -> {all_or_one, res} end))
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

  defp stream_all(
         tenant,
         adapter_meta,
         context,
         %Ecto.Query{
           select: %Ecto.Query.SelectExpr{
             fields: select_fields
           },
           from: %Ecto.Query.FromExpr{source: {source, schema}},
           wheres: wheres
         },
         params,
         options
       ) do
    field_names = Fields.parse_select_fields(select_fields)

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
        plan: plan,
        select_fields: select_fields,
        continuation: nil
      }
    end

    next_fun =
      fn
        acc = %{continuation: %Query.Continuation{more?: false}} ->
          {:halt, acc}

        acc = %{plan: plan, continuation: continuation} ->
          future = Query.all(tenant, adapter_meta, plan, query_options.(continuation))

          future =
            Future.apply(future, fn {objs, continuation} ->
              {[select(objs, field_names)], %{acc | continuation: continuation}}
            end)

          # We can't carry the future beyond this point, because we need the continuation.
          # It would be unusual to use stream inside a transaction anyway.
          Future.result(future)
      end

    after_fun = fn _acc ->
      :ok
    end

    Stream.resource(start_fun, next_fun, after_fun)
  end

  defp select(objs, []) do
    Enum.to_list(objs)
  end

  defp select(objs, select_field_names) do
    rows =
      objs
      |> Stream.map(fn data_object -> Fields.arrange(data_object, select_field_names) end)
      |> Fields.strip_field_names_for_ecto()
      |> Enum.to_list()

    {length(rows), rows}
  end
end
