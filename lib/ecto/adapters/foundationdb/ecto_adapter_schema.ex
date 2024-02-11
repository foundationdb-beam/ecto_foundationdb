defmodule Ecto.Adapters.FoundationDB.EctoAdapterSchema do
  @behaviour Ecto.Adapter.Schema

  alias Ecto.Adapters.FoundationDB, as: FDB

  alias Ecto.Adapters.FoundationDB.Record.Fields
  alias Ecto.Adapters.FoundationDB.Record.Tx
  alias Ecto.Adapters.FoundationDB.Schema
  alias Ecto.Adapters.FoundationDB.Tenant
  alias Ecto.Adapters.FoundationDB.Exception.IncorrectTenancy
  alias Ecto.Adapters.FoundationDB.Exception.Unsupported
  alias Ecto.Adapters.FoundationDB.EctoAdapterMigration

  @impl Ecto.Adapter.Schema
  def autogenerate(:binary_id), do: Ecto.UUID.generate()

  def autogenerate(type),
    do: raise("FoundationDB Adapter does not support autogenerating #{type}")

  @impl Ecto.Adapter.Schema
  def insert_all(
        _adapter_meta = %{opts: adapter_opts},
        schema_meta,
        _header,
        entries,
        _on_conflict,
        _returning,
        _placeholders,
        _options
      ) do
    %{source: source, schema: schema, prefix: tenant} = assert_tenancy!(adapter_opts, schema_meta)

    entries =
      Enum.map(entries, fn fields ->
        pk_field = Fields.get_pk_field!(schema)
        pk = fields[pk_field]
        {pk, fields}
      end)

    num_ins = Tx.insert_all(tenant, adapter_opts, source, entries)
    {num_ins, nil}
  end

  @impl Ecto.Adapter.Schema
  def insert(
        adapter_meta,
        schema_meta,
        fields,
        on_conflict,
        returning,
        options
      ) do
    {1, nil} =
      insert_all(adapter_meta, schema_meta, nil, [fields], on_conflict, returning, [], options)

    {:ok, []}
  end

  @impl Ecto.Adapter.Schema
  def update(
        _adapter_meta = %{opts: adapter_opts},
        schema_meta,
        fields,
        filters,
        _returning,
        _options
      ) do
    %{source: source, schema: schema, prefix: tenant} = assert_tenancy!(adapter_opts, schema_meta)
    pk_field = Fields.get_pk_field!(schema)
    pk = filters[pk_field]

    case Tx.update_pks(tenant, adapter_opts, source, [pk], fields) do
      1 ->
        {:ok, []}

      0 ->
        {:error, :stale}
    end
  end

  @impl Ecto.Adapter.Schema
  def delete(
        _adapter_meta = %{opts: adapter_opts},
        schema_meta,
        filters,
        _returning,
        _options
      ) do
    %{source: source, schema: schema, prefix: tenant} = assert_tenancy!(adapter_opts, schema_meta)
    pk_field = Fields.get_pk_field!(schema)
    pk = filters[pk_field]

    case Tx.delete_pks(tenant, adapter_opts, source, [pk]) do
      1 ->
        {:ok, []}

      0 ->
        {:error, :stale}
    end
  end

  defp assert_tenancy!(
         adapter_opts,
         schema_meta = %{source: source, schema: schema, prefix: tenant}
       ) do
    {context, schema_meta = %{schema: schema, prefix: tenant}} =
      case EctoAdapterMigration.prepare_source(source) do
        {:ok, {source, context}} ->
          tenant =
            if is_binary(tenant),
              do: Tenant.open!(FDB.db(adapter_opts), tenant, adapter_opts),
              else: tenant

          {context, %{schema_meta | source: source, schema: schema, prefix: tenant}}

        {:error, :unknown_source} ->
          context = Schema.get_context!(source, schema)
          {context, schema_meta}
      end

    case Tx.is_safe?(tenant, context[:usetenant]) do
      {false, :unused_tenant} ->
        raise IncorrectTenancy, """
        FoundatioDB Adapter is expecting the struct for schema \
        #{inspect(schema)} to specify no tentant in the prefix metadata, \
        but a non-nil prefix was provided.

        Add `usetenant: true` to your schema's `@schema_context`.

        Also be sure to remove the option `prefix: tenant` on the call to your Repo.

        Alternatively, remove the call to \
        `Ecto.Adapters.FoundationDB.usetenant(struct, tenant)` before inserting.
        """

      {false, :missing_tenant} ->
        raise IncorrectTenancy, """
        FoundationDB Adapter is expecting the struct for schema \
        #{inspect(schema)} to include a tenant in the prefix metadata, \
        but a nil prefix was provided.

        Call `Ecto.Adapters.FoundationDB.usetenant(struxt, tenant)` before inserting.

        Or use the option `prefix: tenant` on the call to your Repo.

        Alternatively, remove `usetenant: true` from your schema's \
        `@schema_context` if you do not want to use a tenant for this schema.
        """

      {false, :tenant_only} ->
        raise Unsupported, "Non-tenant transactions are not yet implemented."

      {false, :tenant_id} ->
        raise Unsupported, "You must provide an open tenant, not a tenant ID"

      true ->
        schema_meta
    end
  end
end
