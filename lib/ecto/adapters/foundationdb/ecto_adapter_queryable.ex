defmodule Ecto.Adapters.FoundationDB.EctoAdapterQueryable do
  @behaviour Ecto.Adapter.Queryable

  alias Ecto.Adapters.FoundationDB, as: FDB

  alias Ecto.Adapters.FoundationDB.EctoAdapterMigration
  alias Ecto.Adapters.FoundationDB.Record.Ordering
  alias Ecto.Adapters.FoundationDB.Record.Fields
  alias Ecto.Adapters.FoundationDB.Record.Tx
  alias Ecto.Adapters.FoundationDB.Schema
  alias Ecto.Adapters.FoundationDB.Tenant
  alias Ecto.Adapters.FoundationDB.Exception.IncorrectTenancy
  alias Ecto.Adapters.FoundationDB.Exception.Unsupported

  @impl Ecto.Adapter.Queryable
  def prepare(
        operation,
        %Ecto.Query{
          order_bys: order_bys,
          limit: limit
        } = query
      ) do
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
          {:nocache, {:all, query, {_limit, limit_fn}, %{}, ordering_fn}},
        params,
        _options
      ) do
    query = %Ecto.Query{prefix: tenant} = assert_tenancy!(query, adapter_opts)

    result =
      tenant
      |> Tx.all(adapter_opts, query, params)
      |> ordering_fn.()
      |> limit_fn.()
      |> Fields.strip_field_names_for_ecto()

    {length(result), result}
  end

  def execute(
        _adapter_meta = %{opts: adapter_opts},
        _query_meta,
        _query_cache =
          {:nocache,
           {:delete_all, query, {nil, _limit_fn}, %{}, _ordering_fn}},
        params,
        _options
      ) do
    query = %Ecto.Query{prefix: tenant} = assert_tenancy!(query, adapter_opts)
    num = Tx.delete_all(tenant, adapter_opts, query, params)
    {num, []}
  end

  @impl Ecto.Adapter.Queryable
  def stream(adapter_meta, query_meta, query_cache, params, options) do
    raise "stream #{inspect(adapter_meta)} #{inspect(query_meta)} #{inspect(query_cache)} #{inspect(params)} #{inspect(options)}"
  end

  # Extract limit from an `Ecto.Query`
  defp get_limit(nil), do: nil
  defp get_limit(%Ecto.Query.QueryExpr{expr: limit}), do: limit

  defp assert_tenancy!(query=%Ecto.Query{
          prefix: tenant,
          from: %Ecto.Query.FromExpr{source: {source, schema}}
  }, adapter_opts) do
    context = Schema.get_context!(source, schema)

    case Tx.is_safe?(tenant, context[:usetenant]) do
      {false, :unused_tenant} ->
        raise IncorrectTenancy, """
        FoundatioDB Adapter is expecting the query for schema \
        #{inspect(schema)} to specify no tentant in the prefix metadata, \
        but a non-nil prefix was provided.

        Add `usetenant: true` to your schema's `@schema_context`.

        Alternatively, remove the `prefix: tenant` from your query.
        """

      {false, :missing_tenant} ->
        raise IncorrectTenancy, """
        FoundationDB Adapter is expecting the query for schema \
        #{inspect(schema)} to include a tenant in the prefix metadata, \
        but a nil prefix was provided.

        Use `prefix: tenant` in your query.

        Alternatively, remove `usetenant: true` from your schema's \
        `@schema_context` if you do not want to use a tenant for this schema.
        """

      {false, :tenant_only} ->
        raise Unsupported, "Non-tenant transactions are not yet implemented."

      {false, :tenant_id} ->
        if not EctoAdapterMigration.is_migration_source?(source) do
          raise Unsupported, "You must provide an open tenant, not a tenant ID"
        end

       tenant = Tenant.open!(FDB.db(adapter_opts), tenant, adapter_opts)
       %Ecto.Query{query | prefix: tenant}

      true ->
        query
      end

  end
end
