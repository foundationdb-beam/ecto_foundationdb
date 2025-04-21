defmodule Ecto.Adapters.FoundationDB.EctoAdapterSchema do
  @moduledoc false
  @behaviour Ecto.Adapter.Schema

  alias EctoFoundationDB.Exception.IncorrectTenancy
  alias EctoFoundationDB.Exception.Unsupported
  alias EctoFoundationDB.Future
  alias EctoFoundationDB.Layer.Fields
  alias EctoFoundationDB.Layer.Metadata
  alias EctoFoundationDB.Layer.Tx
  alias EctoFoundationDB.Schema
  alias EctoFoundationDB.Tenant

  @impl Ecto.Adapter.Schema
  def autogenerate(:binary_id), do: Ecto.UUID.generate()

  def autogenerate(type),
    do: raise("FoundationDB Adapter does not support autogenerating #{type}")

  @impl Ecto.Adapter.Schema
  def insert_all(
        adapter_meta,
        schema_meta,
        _header,
        entries,
        _on_conflict,
        _returning,
        _placeholders,
        options
      ) do
    %{source: source, schema: schema, prefix: tenant, context: context} =
      assert_tenancy!(schema_meta)

    entries =
      Enum.map(entries, fn data_object ->
        pk_field = Fields.get_pk_field!(schema)
        pk = data_object[pk_field]

        if is_nil(pk) do
          raise Unsupported, """
          FoundationDB Adapter does not support inserting records with nil primary keys.
          """
        end

        future = Future.before_transactional()
        {{pk_field, pk}, future, data_object}
      end)

    num_ins =
      Metadata.transactional(tenant, adapter_meta, source, fn tx, metadata ->
        Tx.insert_all(
          tenant,
          tx,
          {schema, source, context},
          entries,
          metadata,
          options
        )
      end)

    {num_ins, nil}
  end

  @impl Ecto.Adapter.Schema
  def insert(adapter_meta, schema_meta, data_object, on_conflict, returning, options) do
    {_count, nil} =
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
        adapter_meta,
        schema_meta,
        update_data,
        filters,
        _returning,
        options
      ) do
    %{source: source, schema: schema, prefix: tenant, context: context} =
      assert_tenancy!(schema_meta)

    pk_field = Fields.get_pk_field!(schema)
    pk = filters[pk_field]
    future = Future.before_transactional()

    res =
      Metadata.transactional(tenant, adapter_meta, source, fn tx, metadata ->
        Tx.update_pks(
          tenant,
          tx,
          {schema, source, context},
          pk_field,
          [{pk, future}],
          update_data,
          metadata,
          options
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
        adapter_meta,
        schema_meta,
        filters,
        _returning,
        _options
      ) do
    %{source: source, schema: schema, prefix: tenant, context: context} =
      assert_tenancy!(schema_meta)

    pk_field = Fields.get_pk_field!(schema)
    pk = filters[pk_field]
    future = Future.before_transactional()

    res =
      Metadata.transactional(tenant, adapter_meta, source, fn tx, metadata ->
        Tx.delete_pks(
          tenant,
          tx,
          {schema, source, context},
          [{pk, future}],
          metadata
        )
      end)

    case res do
      1 ->
        {:ok, []}

      0 ->
        {:error, :stale}
    end
  end

  def watch(_module, _repo, struct, {adapter_meta, options}) do
    # This is not an Ecto callback, so we have to construct our own schema_meta
    schema_meta = %{
      schema: struct.__struct__,
      source: Ecto.get_meta(struct, :source),
      prefix: Keyword.get(options, :prefix, Ecto.get_meta(struct, :prefix))
    }

    %{schema: schema, source: source, context: context, prefix: tenant} =
      assert_tenancy!(schema_meta)

    pk_field = Fields.get_pk_field!(schema)
    pk = Map.get(struct, pk_field)

    Metadata.transactional(tenant, adapter_meta, source, fn tx, _metadata ->
      future_ref = Tx.watch(tenant, tx, {schema, source, context}, {pk_field, pk}, options)
      Future.new_deferred(future_ref, fn _ -> {schema, pk, options} end)
    end)
  end

  defp assert_tenancy!(schema_meta = %{source: source, schema: schema, prefix: tenant}) do
    schema_meta = Map.put(schema_meta, :context, Schema.get_context!(source, schema))

    case Tx.safe?(tenant) do
      {false, :missing_tenant} ->
        raise IncorrectTenancy, """
        FoundationDB Adapter is expecting the struct for schema \
        #{inspect(schema)} to include a tenant in the prefix metadata, \
        but a nil prefix was provided.

        Call `Ecto.Adapters.FoundationDB.usetenant(struct, tenant)` before inserting.

        Or use the option `prefix: tenant` on the call to your Repo.
        """

      {true, tenant = %Tenant{}} ->
        Map.put(schema_meta, :prefix, tenant)
    end
  end
end
