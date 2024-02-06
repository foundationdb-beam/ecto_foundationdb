defmodule Ecto.Adapters.FoundationDB.EctoAdapterQueryable do
  @behaviour Ecto.Adapter.Queryable

  alias Ecto.Adapters.FoundationDB.Record.Ordering
  alias Ecto.Adapters.FoundationDB.Record.Transaction

  @impl Ecto.Adapter.Queryable
  def prepare(
        operation,
        %Ecto.Query{
          prefix: tenant,
          from: %Ecto.Query.FromExpr{source: {_source, schema}},
          order_bys: order_bys,
          limit: limit
        } = query
      ) do
    context = get_context!(schema)

    if context[:usetenant] and is_nil(tenant) do
      raise """
      FoundationDB Adapter is expecting the query for schema \
      #{inspect(schema)} to include a tenant in the prefix metadata, \
      but a nil prefix was provided.

      Use `prefix: tenant` in your query.

      Alternatively, remove `usetenant: true` from your schema's \
      `@schema_context` if you do not want to use a tenant for this schema.
      """
    end

    if is_nil(context[:usetenant] and not is_nil(tenant)) do
      raise """
      FoundatioDB Adapter is expecting the query for schema \
      #{inspect(schema)} to specify no tentant in the prefix metadata, \
      but a non-nil prefix was provided.

      Add `usetenant: true` to your schema's `@schema_context`.

      Alternatively, remove the `prefix: tenant` from your query.
      """
    end

    if is_nil(tenant) do
      raise "Non-tenant query transactions are not yet implemented."
    end

    ordering_fn = Ordering.get_ordering_fn(order_bys)
    limit = get_limit(limit)
    limit_fn = if limit == nil, do: & &1, else: &Enum.take(&1, limit)
    {:nocache, {operation, query, {limit, limit_fn}, %{}, ordering_fn}}
  end

  @impl Ecto.Adapter.Queryable
  def execute(
        _adapter_meta = %{opts: adapter_opts},
        _query_meta,
        _query_cache =
          {:nocache,
           {:all, %Ecto.Query{prefix: tenant} = query, {_limit, limit_fn}, %{}, ordering_fn}},
        params,
        _options
      ) do
    result =
      tenant
      |> :erlfdb.transactional(Transaction.all(adapter_opts, query, params))
      |> ordering_fn.()
      |> limit_fn.()

    {length(result), result}
  end

  @impl Ecto.Adapter.Queryable
  def stream(adapter_meta, query_meta, query_cache, params, options) do
    raise "stream #{inspect(adapter_meta)} #{inspect(query_meta)} #{inspect(query_cache)} #{inspect(params)} #{inspect(options)}"
  end

  # Extract limit from an `Ecto.Query`
  defp get_limit(nil), do: nil
  defp get_limit(%Ecto.Query.QueryExpr{expr: limit}), do: limit

  defp get_context!(schema) do
    %{__meta__: %{context: context}} = Kernel.struct!(schema)
    context
  end
end
