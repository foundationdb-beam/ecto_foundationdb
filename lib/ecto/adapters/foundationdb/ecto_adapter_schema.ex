defmodule Ecto.Adapters.FoundationDB.EctoAdapterSchema do
  @moduledoc false
  @behaviour Ecto.Adapter.Schema

  alias EctoFoundationDB.Exception.IncorrectTenancy
  alias EctoFoundationDB.Exception.Unsupported
  alias EctoFoundationDB.Layer.Fields
  alias EctoFoundationDB.Layer.IndexInventory
  alias EctoFoundationDB.Layer.Tx
  alias EctoFoundationDB.Schema

  @impl Ecto.Adapter.Schema
  def autogenerate(:binary_id), do: Ecto.UUID.generate()

  def autogenerate(type),
    do: raise("FoundationDB Adapter does not support autogenerating #{type}")

  @impl Ecto.Adapter.Schema
  def insert_all(
        adapter_meta = %{opts: adapter_opts},
        schema_meta,
        _header,
        entries,
        _on_conflict,
        _returning,
        _placeholders,
        _options
      ) do
    %{source: source, schema: schema, prefix: tenant, context: context} =
      assert_tenancy!(adapter_opts, schema_meta)

    entries =
      Enum.map(entries, fn data_object ->
        pk_field = Fields.get_pk_field!(schema)
        pk = data_object[pk_field]
        {{pk_field, pk}, data_object}
      end)

    num_ins =
      IndexInventory.transactional(tenant, adapter_meta, source, fn tx, idxs, partial_idxs ->
        Tx.insert_all(tx, {schema, source, context}, entries, idxs, partial_idxs)
      end)

    {num_ins, nil}
  end

  @impl Ecto.Adapter.Schema
  def insert(adapter_meta, schema_meta, data_object, on_conflict, returning, options) do
    {1, nil} =
      insert_all(
        adapter_meta,
        schema_meta,
        nil,
        [data_object],
        on_conflict,
        returning,
        [],
        options
      )

    {:ok, []}
  end

  @impl Ecto.Adapter.Schema
  def update(
        adapter_meta = %{opts: adapter_opts},
        schema_meta,
        update_data,
        filters,
        _returning,
        _options
      ) do
    %{source: source, schema: schema, prefix: tenant, context: context} =
      assert_tenancy!(adapter_opts, schema_meta)

    pk_field = Fields.get_pk_field!(schema)
    pk = filters[pk_field]

    res =
      IndexInventory.transactional(tenant, adapter_meta, source, fn tx, idxs, partial_idxs ->
        Tx.update_pks(
          tx,
          {schema, source, context},
          pk_field,
          [pk],
          update_data,
          idxs,
          partial_idxs
        )
      end)

    case res do
      1 ->
        {:ok, []}

      0 ->
        {:error, :stale}
    end
  end

  @impl Ecto.Adapter.Schema
  def delete(
        adapter_meta = %{opts: adapter_opts},
        schema_meta,
        filters,
        _returning,
        _options
      ) do
    %{source: source, schema: schema, prefix: tenant, context: context} =
      assert_tenancy!(adapter_opts, schema_meta)

    pk_field = Fields.get_pk_field!(schema)
    pk = filters[pk_field]

    res =
      IndexInventory.transactional(tenant, adapter_meta, source, fn tx, idxs, partial_idxs ->
        Tx.delete_pks(tx, {schema, source, context}, [pk], idxs, partial_idxs)
      end)

    case res do
      1 ->
        {:ok, []}

      0 ->
        {:error, :stale}
    end
  end

  defp assert_tenancy!(
         _adapter_opts,
         schema_meta = %{source: source, schema: schema}
       ) do
    schema_meta =
      %{schema: schema, prefix: tenant, context: context} =
      Map.put(schema_meta, :context, Schema.get_context!(source, schema))

    case Tx.safe?(tenant, Schema.get_option(context, :usetenant)) do
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

      true ->
        schema_meta
    end
  end
end
