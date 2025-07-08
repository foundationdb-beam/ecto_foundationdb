defmodule EctoFoundationDB.Indexer.SchemaMetadata do
  @moduledoc false

  alias EctoFoundationDB.Exception.Unsupported
  alias EctoFoundationDB.Future
  alias EctoFoundationDB.Indexer
  alias EctoFoundationDB.Layer.Pack
  alias EctoFoundationDB.Layer.Tx
  alias EctoFoundationDB.Schema

  @behaviour Indexer

  def inserts(schema, opts), do: sync_get(schema, :inserts, opts)
  def deletes(schema, opts), do: sync_get(schema, :deletes, opts)
  def collection(schema, opts), do: sync_get(schema, :collection, opts)
  def updates(schema, opts), do: sync_get(schema, :updates, opts)
  def changes(schema, opts), do: sync_get(schema, :changes, opts)

  def watch_inserts(schema, opts \\ []), do: watch(schema, :inserts, opts)
  def watch_deletes(schema, opts \\ []), do: watch(schema, :deletes, opts)
  def watch_collection(schema, opts \\ []), do: watch(schema, :collection, opts)
  def watch_updates(schema, opts \\ []), do: watch(schema, :updates, opts)
  def watch_changes(schema, opts \\ []), do: watch(schema, :changes, opts)

  def async_inserts(schema), do: async_get(schema, :inserts)
  def async_deletes(schema), do: async_get(schema, :deletes)
  def async_collection(schema), do: async_get(schema, :collection)
  def async_updates(schema), do: async_get(schema, :updates)
  def async_changes(schema), do: async_get(schema, :changes)

  defp sync_get(schema, name, prefix: tenant),
    do: Tx.transactional(tenant, fn _ -> schema |> async_get(name) |> Future.result() end)

  defp async_get(schema, name) when is_atom(schema) do
    async_get(Schema.get_source(schema), name)
  end

  defp async_get(source, name) do
    future = Future.new()
    {tenant, tx} = assert_tenant_tx!()
    future_ref = :erlfdb.get(tx, key(tenant, source, name))
    Future.set(future, tx, future_ref, &decode_counter/1)
  end

  defp watch(schema, name, opts) do
    {tenant, tx} = assert_tenant_tx!()
    source = Schema.get_source(schema)
    future_ref = :erlfdb.watch(tx, key(tenant, source, name))

    Future.new_deferred(
      future_ref,
      fn _ ->
        {schema, {__MODULE__, name}, opts,
         fn _, new_opts -> watch(schema, name, Keyword.merge(opts, new_opts)) end}
      end
    )
  end

  # This defines the full set of possible values for the `include` param
  # when creating the schema metadata index.
  #
  # It might feel natural to add a `:count` field. However, this can't be done
  # accurately due to upserts.
  def field_names() do
    [
      # A counter that's incremented whenever a new insert occurs. The counter value
      # is only meaningful when compared relative to a past value.
      :inserts,

      # A counter that's incremented whenever a new delete occurs. The counter value
      # is only meaningful when compared relative to a past value.
      :deletes,

      # A counter that's incremented whenever a new insert or delete occurs. The counter value
      # is only meaningful when compared relative to a past value.
      :collection,

      # A counter that's incremented whenever a new update occurs. The counter value
      # is only meaningful when compared relative to a past value.
      :updates,

      # A counter that's incremented whenever a new change occurs. The counter value
      # is only meaningful when compared relative to a past value.
      # A change is defined as any of: insert, delete, or update.
      :changes
    ]
  end

  @impl true
  def create_range(tenant, idx) do
    Pack.primary_range(tenant, idx[:source])
  end

  @impl true
  def drop_ranges(tenant, idx) do
    for f <- idx[:fields], do: key(tenant, idx[:source], f)
  end

  @impl true
  def create(_tenant, _tx, _idx, _schema, {_start_key, end_key}, _limit) do
    {0, {end_key, end_key}}
  end

  @impl true
  def set(tenant, tx, idx, _schema, _kv) do
    add(tenant, tx, idx, :inserts)
    add(tenant, tx, idx, :collection)
    add(tenant, tx, idx, :changes)
    :ok
  end

  @impl true
  def update(tenant, tx, idx, _schema, _kv, _updates) do
    add(tenant, tx, idx, :updates)
    add(tenant, tx, idx, :changes)
    :ok
  end

  @impl true
  def clear(tenant, tx, idx, _schema, _kv) do
    add(tenant, tx, idx, :deletes)
    add(tenant, tx, idx, :collection)
    add(tenant, tx, idx, :changes)
    :ok
  end

  @impl true
  def range(_idx, _plan, _options) do
    raise Unsupported, """
    SchemaMetadata cannot be queried by Ecto.
    """
  end

  defp key(tenant, source, name) do
    Pack.schema_metadata_pack(tenant, source, name)
  end

  defp add(tenant, tx, idx, name, num \\ 1) do
    if name in idx[:fields] do
      :erlfdb.add(tx, key(tenant, idx[:source], name), num)
    end
  end

  defp decode_counter(:not_found), do: 0
  defp decode_counter(x), do: :binary.decode_unsigned(x, :little)

  defp assert_tenant_tx!() do
    tenant =
      case Tx.in_tenant_tx?() do
        {true, tenant} ->
          tenant

        {false, _} ->
          raise Unsupported, """
          SchemaMetadata functions must be executed for a specific tenant.
          """
      end

    tx =
      if Tx.in_tx?() do
        Tx.get()
      else
        raise Unsupported, """
        SchemaMetadata functions must be executed within a transaction.
        """
      end

    {tenant, tx}
  end
end
