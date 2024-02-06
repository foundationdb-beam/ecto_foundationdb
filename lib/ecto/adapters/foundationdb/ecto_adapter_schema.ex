defmodule Ecto.Adapters.FoundationDB.EctoAdapterSchema do
  @behaviour Ecto.Adapter.Schema

  alias Ecto.Adapters.FoundationDB.Record.Fields
  alias Ecto.Adapters.FoundationDB.Record.Transaction

  @impl Ecto.Adapter.Schema
  def autogenerate(:binary_id), do: Ecto.UUID.generate()

  def autogenerate(type),
    do: raise("FoundationDB Adapter does not support autogenerating #{type}")

  @impl Ecto.Adapter.Schema
  def insert_all(
        _adapter_meta,
        _schema_meta,
        _header,
        _unsure,
        _on_conflict,
        _returning,
        _placeholders,
        _options
      ) do
    raise "FoundationmDB Adapter insert_all not implemented"
  end

  @impl Ecto.Adapter.Schema
  def insert(
        _adapter_meta = %{opts: adapter_opts},
        _schema_meta = %{context: context, prefix: tenant, source: source, schema: schema},
        fields,
        _on_conflict,
        _returning,
        _options
      ) do
    if context[:usetenant] and is_nil(tenant) do
      raise """
      FoundationDB Adapter is expecting the struct for schema \
      #{inspect(schema)} to include a tenant in the prefix metadata, \
      but a nil prefix was provided.

      Call `Ecto.Adapters.FoundationDB.usetenant(struxt, tenant)` before inserting.

      Alternatively, remove `usetenant: true` from your schema's \
      `@schema_context` if you do not want to use a tenant for this schema.
      """
    end

    if is_nil(context[:usetenant] and not is_nil(tenant)) do
      raise """
      FoundatioDB Adapter is expecting the struct for schema \
      #{inspect(schema)} to specify no tentant in the prefix metadata, \
      but a non-nil prefix was provided.

      Add `usetenant: true` to your schema's `@schema_context`.

      Alternatively, remove the call to \
      `Ecto.Adapters.FoundationDB.usetenant(struct, tenant)` before inserting.
      """
    end

    if is_nil(tenant) do
      raise "Non-tenant transactions are not yet implemented."
    end

    # %{pid: #PID<0.283.0>, opts: [repo: Ecto.Integration.TestRepo, telemetry_prefix: [:ecto, :integration, :test_repo], otp_app: :ecto_foundationdb, timeout: 15000, pool_size: 10], cache: #Reference<0.1764422960.586285060.230287>, stacktrace: nil, repo: Ecto.Integration.TestRepo, telemetry: {Ecto.Integration.TestRepo, :debug, [:ecto, :integration, :test_repo, :query]}, adapter: Ecto.Adapters.FoundationDB}
    # %{context: nil, prefix: nil, source: "users", schema: EctoFoundationDB.Schemas.User, autogenerate_id: {:id, :id, :binary_id}}
    # [name: "John", inserted_at: ~N[2024-02-05 23:48:10], updated_at: ~N[2024-02-05 23:48:10], id: "96aaa43b-370f-4ae4-bb18-0120a46d9dab"]
    # {:raise, [], []}
    # []
    # [cast_params: ["John", ~N[2024-02-05 23:48:10], ~N[2024-02-05 23:48:10], "96aaa43b-370f-4ae4-bb18-0120a46d9dab"]]

    pk_field = Fields.get_pk_field!(schema)
    pk = fields[pk_field]

    :ok = :erlfdb.transactional(tenant, Transaction.insert(adapter_opts, source, pk_field, pk, fields))
    {:ok, []}
  end

  @impl Ecto.Adapter.Schema
  def update(adapter_meta, schema_meta, fields, filters, returning, options) do
    raise "FoundationDB Adapter update #{inspect(adapter_meta)} #{inspect(schema_meta)} #{inspect(fields)} #{inspect(filters)} #{inspect(returning)} #{inspect(options)}"
  end

  @impl Ecto.Adapter.Schema
  def delete(adapter_meta, schema_meta, filters, returning, options) do
    raise "FoundationDB Adapter delete #{inspect(adapter_meta)} #{inspect(schema_meta)} #{inspect(filters)} #{inspect(returning)} #{inspect(options)}"
  end
end
