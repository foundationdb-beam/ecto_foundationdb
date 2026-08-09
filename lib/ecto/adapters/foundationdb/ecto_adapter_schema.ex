defmodule Ecto.Adapters.FoundationDB.EctoAdapterSchema do
  @moduledoc false
  @behaviour Ecto.Adapter.Schema

  alias EctoFoundationDB.Assert.CorrectTenancy
  alias EctoFoundationDB.Exception.Unsupported
  alias EctoFoundationDB.Future
  alias EctoFoundationDB.Layer.Fields
  alias EctoFoundationDB.Layer.Fields.CompositePK
  alias EctoFoundationDB.Layer.Metadata
  alias EctoFoundationDB.Layer.Tx

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
      CorrectTenancy.assert_by_schema!(adapter_meta[:opts], schema_meta)

    entries =
      Enum.map(entries, fn data_object ->
        pk_fields = Fields.get_pk_fields!(schema)
        pk = Fields.get_pk_value!(schema, data_object)

        if nil_pk?(pk) do
          raise Unsupported, """
          FoundationDB Adapter does not support inserting records with nil primary keys.
          """
        end

        {{pk_fields, pk}, data_object}
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
      CorrectTenancy.assert_by_schema!(adapter_meta[:opts], schema_meta)

    pk_fields = Fields.get_pk_fields!(schema)
    pk = Fields.get_pk_value!(schema, filters)

    res =
      Metadata.transactional(tenant, adapter_meta, source, fn tx, metadata ->
        Tx.update_pks(
          tenant,
          tx,
          {schema, source, context},
          pk_fields,
          [pk],
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
      CorrectTenancy.assert_by_schema!(adapter_meta[:opts], schema_meta)

    pk = Fields.get_pk_value!(schema, filters)

    res =
      Metadata.transactional(tenant, adapter_meta, source, fn tx, metadata ->
        Tx.delete_pks(
          tenant,
          tx,
          {schema, source, context},
          [pk],
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

  def watch(_module, _repo, nil, {_adapter_meta, _options}) do
    nil
  end

  def watch(module, repo, struct, {adapter_meta, options}) do
    schema = struct.__struct__

    if Fields.composite_pk?(schema) do
      raise Unsupported, """
      `Repo.watch` is not supported for schemas with composite primary keys.

      Schema: #{inspect(schema)}

      This is a temporary limitation. Future enhancements could add support for watching composite key records.
      """
    end

    # This is not an Ecto callback, so we have to construct our own schema_meta
    schema_meta = %{
      schema: schema,
      source: Ecto.get_meta(struct, :source),
      prefix: Keyword.get(options, :prefix, Ecto.get_meta(struct, :prefix))
    }

    %{schema: schema, source: source, context: context, prefix: tenant} =
      CorrectTenancy.assert_by_schema!(adapter_meta[:opts], schema_meta)

    pk_fields = Fields.get_pk_fields!(schema)
    pk = Fields.get_pk_value!(schema, struct)

    Tx.transactional(tenant, fn tx ->
      erlfdb_future = Tx.watch(tenant, tx, {schema, source, context}, {pk_fields, pk}, options)

      # See EctoAdapterAssigns for the other half of this implementation.
      Future.new(:erlfdb_future, erlfdb_future, fn _ ->
        {schema, {:pk, pk}, options,
         &watch(module, repo, &1, {adapter_meta, Keyword.merge(options, &2)})}
      end)
    end)
  end

  # A partial composite key identifies no single record.
  defp nil_pk?(%CompositePK{values: ids}), do: Enum.any?(ids, &is_nil/1)
  defp nil_pk?(pk), do: is_nil(pk)
end
