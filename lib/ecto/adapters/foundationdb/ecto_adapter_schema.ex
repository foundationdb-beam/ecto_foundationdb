defmodule Ecto.Adapters.FoundationDB.EctoAdapterSchema do
  @behaviour Ecto.Adapter.Schema

  alias Ecto.Adapters.FoundationDB.Record.Fields
  alias Ecto.Adapters.FoundationDB.Record.Tx
  alias Ecto.Adapters.FoundationDB.Schema
  alias Ecto.Adapters.FoundationDB.Exception.IncorrectTenancy
  alias Ecto.Adapters.FoundationDB.Exception.Unsupported

  @impl Ecto.Adapter.Schema
  def autogenerate(:binary_id), do: Ecto.UUID.generate()

  def autogenerate(type),
    do: raise("FoundationDB Adapter does not support autogenerating #{type}")

  @impl Ecto.Adapter.Schema
  def insert_all(
        _adapter_meta = %{opts: adapter_opts},
        _schema_meta = %{prefix: tenant, source: source, schema: schema},
        _header,
        entries,
        _on_conflict,
        _returning,
        _placeholders,
        _options
      ) do
    # %{pid: #PID<0.283.0>, opts: [repo: Ecto.Integration.TestRepo, telemetry_prefix: [:ecto, :integration, :test_repo], otp_app: :ecto_foundationdb, timeout: 15000, pool_size: 10], cache: #Reference<0.1764422960.586285060.230287>, stacktrace: nil, repo: Ecto.Integration.TestRepo, telemetry: {Ecto.Integration.TestRepo, :debug, [:ecto, :integration, :test_repo, :query]}, adapter: Ecto.Adapters.FoundationDB}
    # %{context: nil, prefix: nil, source: "users", schema: EctoFoundationDB.Schemas.User, autogenerate_id: {:id, :id, :binary_id}}
    # [name: "John", inserted_at: ~N[2024-02-05 23:48:10], updated_at: ~N[2024-02-05 23:48:10], id: "96aaa43b-370f-4ae4-bb18-0120a46d9dab"]
    # {:raise, [], []}
    # []
    # [cast_params: ["John", ~N[2024-02-05 23:48:10], ~N[2024-02-05 23:48:10], "96aaa43b-370f-4ae4-bb18-0120a46d9dab"]]

    assert_tenancy!(schema, tenant)

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
  def update(adapter_meta, schema_meta, fields, filters, returning, options) do
    raise "FoundationDB Adapter update #{inspect(adapter_meta)} #{inspect(schema_meta)} #{inspect(fields)} #{inspect(filters)} #{inspect(returning)} #{inspect(options)}"
  end

  @impl Ecto.Adapter.Schema
  def delete(
        _adapter_meta = %{opts: adapter_opts},
        _schema_meta = %{prefix: tenant, source: source, schema: schema},
        filters,
        _returning,
        _options
      ) do
    # %{pid: #PID<0.292.0>, opts: [repo: Ecto.Integration.TestRepo, telemetry_prefix: [:ecto, :integration, :test_repo], otp_app: :ecto_foundationdb, timeout: 15000, pool_size: 10, key_delimiter: "/"], cache: #Reference<0.656434356.2939551747.188470>, stacktrace: nil, repo: Ecto.Integration.TestRepo, telemetry: {Ecto.Integration.TestRepo, :debug, [:ecto, :integration, :test_repo, :query]}, adapter: Ecto.Adapters.FoundationDB}
    # %{context: [usetenant: true], prefix: {:erlfdb_tenant, #Reference<0.656434356.2939551746.189187>}, source: "users", schema: EctoFoundationDB.Schemas.User, autogenerate_id: {:id, :id, :binary_id}}
    # [id: "46866b69-c11b-45a3-a35d-5985067c3f8f"]
    # []
    # [cast_params: ["46866b69-c11b-45a3-a35d-5985067c3f8f"]]
    assert_tenancy!(schema, tenant)
    pk_field = Fields.get_pk_field!(schema)
    pk = filters[pk_field]
    :ok = Tx.delete(tenant, adapter_opts, source, pk)
    # {:ok, fields} | {:invalid, constraints} | {:error, :stale}
    {:ok, []}
  end

  defp assert_tenancy!(schema, tenant) do
    context = Schema.get_context!(schema)

    case Tx.is_safe?(:struct, tenant, context[:usetenant]) do
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
        :ok
    end
  end
end
