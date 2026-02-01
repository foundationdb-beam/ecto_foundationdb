defmodule Ecto.Adapters.FoundationDB.EctoAdapterQueryable do
  @moduledoc false
  @behaviour Ecto.Adapter.Queryable

  alias Ecto.Adapters.FoundationDB.EctoAdapterQueryable.Continuation

  alias EctoFoundationDB.Assert.CorrectTenancy
  alias EctoFoundationDB.Future
  alias EctoFoundationDB.Layer.DecodedKV
  alias EctoFoundationDB.Layer.Fields
  alias EctoFoundationDB.Layer.Query
  alias EctoFoundationDB.Layer.Tx
  alias EctoFoundationDB.QueryPlan
  alias EctoFoundationDB.Schema

  alias FDB.LazyRangeIterator

  defmodule Continuation do
    @moduledoc false
    defstruct more?: false, start_key: nil
  end

  @impl Ecto.Adapter.Queryable
  def prepare(operation, query = %Ecto.Query{}) do
    # :nocache required by Ecto
    {:nocache, {operation, query}}
  end

  @impl Ecto.Adapter.Queryable
  def execute(
        adapter_meta,
        _query_meta,
        {:nocache, {:all, query = %Ecto.Query{}}},
        params,
        options
      ) do
    %{opts: repo_config} = adapter_meta

    %Ecto.Query{
      from: %Ecto.Query.FromExpr{source: {source, schema}},
      wheres: wheres,
      order_bys: order_bys,
      select: %Ecto.Query.SelectExpr{
        fields: select_fields
      },
      limit: limit
    } = query

    case options[:noop] do
      query_result when not is_nil(query_result) ->
        # This is the trick to load structs after a pipelined 'get'. See async_get_by, await, etc
        query_result

      _ ->
        {%{context: context}, %Ecto.Query{prefix: tenant}} =
          CorrectTenancy.assert_by_query!(repo_config, query)

        limit = parse_query_limit(limit)

        plan =
          QueryPlan.get(tenant, source, schema, context, wheres, [], params, order_bys, limit)

        select_fields = Fields.parse_select_fields(select_fields)

        create_query_all_future(tenant, adapter_meta, plan, :all, select_fields, options)
        |> handle_returning(options)
    end
  end

  def execute(
        adapter_meta = %{opts: repo_config},
        _query_meta,
        {:nocache, {:delete_all, query = %Ecto.Query{limit: nil}}},
        params,
        _options
      ) do
    %Ecto.Query{
      from: %Ecto.Query.FromExpr{source: {source, schema}},
      wheres: wheres
    } = query

    {%{context: context}, %Ecto.Query{prefix: tenant}} =
      CorrectTenancy.assert_by_query!(repo_config, query)

    plan = QueryPlan.get(tenant, source, schema, context, wheres, [], params, [], nil)
    num = Query.delete(tenant, adapter_meta, plan)

    {num, []}
  end

  def execute(
        adapter_meta = %{opts: repo_config},
        _query_meta,
        {:nocache, {:update_all, query = %Ecto.Query{limit: nil}}},
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
        {:nocache, {:all, query = %Ecto.Query{limit: nil, order_bys: order_bys}}},
        params,
        options
      )
      when order_bys in [nil, []] do
    {%{context: context}, query = %Ecto.Query{prefix: tenant}} =
      CorrectTenancy.assert_by_query!(repo_config, query)

    stream_all(tenant, adapter_meta, context, query, params, options)
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

    limit =
      case queryable do
        %Ecto.Query{limit: limit} ->
          parse_query_limit(limit)

        _ ->
          nil
      end

    plan = QueryPlan.all_range(tenant, source, schema, context, id_s, id_e, limit, options)

    create_query_all_future(tenant, adapter_meta, plan, return_handler, select_fields, options)
    |> handle_returning(options ++ [returning: {:future, return_handler}])
  end

  defp handle_returning(future, options) do
    case options[:returning] do
      {:future, :all_from_source} ->
        future =
          Future.then(
            future,
            fn result ->
              {select_fields, objs} = result
              {:all_from_source, {select_fields, {length(objs), objs}}}
            end
          )

        Process.put(Future.token(), future)
        {0, []}

      {:future, return_handler} ->
        future =
          Future.then(
            future,
            fn result ->
              {return_handler, {length(result), result}}
            end
          )

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
    plan = QueryPlan.get(tenant, source, schema, context, wheres, updates, params, [], nil)
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
      plan = QueryPlan.get(tenant, source, schema, context, wheres, [], params, [], nil)

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

    compute_cont = fn
      [] ->
        {0, nil}

      pages ->
        last_page = List.last(pages)
        {Enum.sum(Stream.map(pages, &length/1)), List.last(last_page)}
    end

    {cont, stream} =
      Tx.transactional(tenant, fn tx ->
        # Advance the stream so that we can retrieve the last key for building
        # the continuation
        {ri, nil} = Query.all(tenant, adapter_meta, plan, options)
        {cont, ri} = LazyRangeIterator.advance(ri, compute_cont)

        stream = FDB.Stream.from_iterator(ri)

        stream =
          if is_nil(user_callback) do
            stream
          else
            Enum.to_list(user_callback.(tx, stream))
          end

        {cont, stream}
      end)

    objs =
      stream
      |> select(nil, field_names)
      |> Enum.to_list()

    {[{length(objs), objs}], %{acc | continuation: continuation(cont, options)}}
  end

  defp stream_all_after(_acc), do: :ok

  defp get_field_names_union(dkvs) do
    {all_fields, _} =
      dkvs
      |> Enum.reduce({[], MapSet.new()}, fn dkv = %DecodedKV{}, {list, set_a} ->
        %{data_object: data_object} = dkv
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

  defp select(stream, post_query_ordering_fn, select_field_names) do
    stream =
      stream
      |> Stream.map(fn %DecodedKV{data_object: data_object} ->
        Fields.arrange(data_object, select_field_names)
      end)

    enum = if is_nil(post_query_ordering_fn), do: stream, else: post_query_ordering_fn.(stream)

    enum
    |> Fields.strip_field_names_for_ecto()
    |> Enum.to_list()
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

  defp create_query_all_future(tenant, adapter_meta, plan, return_handler, select_fields, options) do
    must_wait? = not Tx.in_tx?()

    {future, post_query_ordering_fn} =
      Tx.transactional(tenant, fn _tx ->
        {ri, post_query_ordering_fn} = Query.all(tenant, adapter_meta, plan, options)
        future = Future.new(:erlfdb_iterator, ri)

        future =
          if must_wait? do
            Future.await(future)
          else
            future
          end

        {future, post_query_ordering_fn}
      end)

    Future.then(future, fn dkvs ->
      case return_handler do
        :all_from_source ->
          dkvs = Enum.to_list(dkvs)
          select_fields = select_fields || get_field_names_union(dkvs)
          objs = select(dkvs, post_query_ordering_fn, select_fields)
          {select_fields, objs}

        _ ->
          select(dkvs, post_query_ordering_fn, select_fields)
      end
    end)
  end

  # Extract limit from an `Ecto.Query`
  defp parse_query_limit(nil), do: nil
  defp parse_query_limit(%Ecto.Query.LimitExpr{expr: limit}), do: limit
end
