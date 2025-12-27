defmodule EctoFoundationDB.Indexer.SchemaMetadata do
  @moduledoc """
  This is Indexer keeps track of various actions for a Schema:

  - `inserts`: Incremented for each insert or upsert
  - `deletes`: Incremented for each delete
  - `collection`: Incremented for each insert, upsert, or delete
  - `updates`: Incremented for each update (via `Repo.update/*`)
  - `changes`: Incremented for each insert, upsert, delete, or update

  These keys are useful for creating watches that will notify your application of those actions. For example, if you create a watch on the `inserts` key,
  your application will be notified when a new record is inserted, and you can react however you like.

  See it in action: [Sync Engine Part II - Collections](collection_syncing.html)
  """

  alias EctoFoundationDB.Exception.Unsupported
  alias EctoFoundationDB.Future
  alias EctoFoundationDB.Indexer
  alias EctoFoundationDB.Layer.Metadata
  alias EctoFoundationDB.Layer.Pack
  alias EctoFoundationDB.Layer.Tx
  alias EctoFoundationDB.QueryPlan
  alias EctoFoundationDB.Schema
  alias EctoFoundationDB.Tenant

  @behaviour Indexer

  @doc """
  Synchronous get on the `inserts` key.
  """
  def inserts(schema, opts), do: inserts_by(schema, [], opts)

  @doc """
  Synchronous get on the `deletes` key.
  """
  def deletes(schema, opts), do: deletes_by(schema, [], opts)

  @doc """
  Synchronous get on the `collection` key.
  """
  def collection(schema, opts), do: collection_by(schema, [], opts)

  @doc """
  Synchronous get on the `updates` key.
  """
  def updates(schema, opts), do: updates_by(schema, [], opts)

  @doc """
  Synchronous get on the `changes` key.
  """
  def changes(schema, opts), do: changes_by(schema, [], opts)

  @doc """
  Synchronous get on the `inserts` key for the provided indexed values.
  """
  def inserts_by(schema, indexed_values, opts),
    do: sync_get(schema, indexed_values, :inserts, opts)

  @doc """
  Synchronous get on the `deletes` key for the provided indexed values.
  """
  def deletes_by(schema, indexed_values, opts),
    do: sync_get(schema, indexed_values, :deletes, opts)

  @doc """
  Synchronous get on the `collection` key for the provided indexed values.
  """
  def collection_by(schema, indexed_values, opts),
    do: sync_get(schema, indexed_values, :collection, opts)

  @doc """
  Synchronous get on the `updates` key for the provided indexed values.
  """
  def updates_by(schema, indexed_values, opts),
    do: sync_get(schema, indexed_values, :updates, opts)

  @doc """
  Synchronous get on the `changes` key for the provided indexed values.
  """
  def changes_by(schema, indexed_values, opts),
    do: sync_get(schema, indexed_values, :changes, opts)

  def clear(schema), do: clear_by(schema, [])

  def clear_by(schema, indexed_values) do
    source = Schema.get_source(schema)
    {tenant, tx} = assert_tenant_tx!()
    idx = tx_lookup_idx!(tenant, source, indexed_values)
    index_name = idx[:id]
    index_values = Indexer.Default.get_index_values(schema, idx[:fields], indexed_values)

    options = Keyword.get(idx, :options, [])
    signals = Keyword.get(options, :signals, signal_names())

    fields = idx[:fields]

    for signal <- signals,
        do: :erlfdb.clear(tx, key(tenant, source, index_name, fields, index_values, signal))
  end

  @doc """
  Equivalent to `watch(schema, :inserts, opts)`
  """
  def watch_inserts(schema, opts \\ []), do: watch(schema, :inserts, opts)

  @doc """
  Equivalent to `watch(schema, :deletes, opts)`
  """
  def watch_deletes(schema, opts \\ []), do: watch(schema, :deletes, opts)

  @doc """
  Equivalent to `watch(schema, :collection, opts)`
  """
  def watch_collection(schema, opts \\ []), do: watch(schema, :collection, opts)

  @doc """
  Equivalent to `watch(schema, :updates, opts)`
  """
  def watch_updates(schema, opts \\ []), do: watch(schema, :updates, opts)

  @doc """
  Equivalent to `watch(schema, :changes, opts)`
  """
  def watch_changes(schema, opts \\ []), do: watch(schema, :changes, opts)

  @doc """
  Equivalent to `watch_by(schema, indexed_values, :inserts, opts)`
  """
  def watch_inserts_by(schema, indexed_values, opts \\ []),
    do: watch_by(schema, indexed_values, :inserts, opts)

  @doc """
  Equivalent to `watch_by(schema, indexed_values, :deletes, opts)`
  """
  def watch_deletes_by(schema, indexed_values, opts \\ []),
    do: watch_by(schema, indexed_values, :deletes, opts)

  @doc """
  Equivalent to `watch_by(schema, indexed_values, :collection, opts)`
  """
  def watch_collection_by(schema, indexed_values, opts \\ []),
    do: watch_by(schema, indexed_values, :collection, opts)

  @doc """
  Equivalent to `watch_by(schema, indexed_values, :updates, opts)`
  """
  def watch_updates_by(schema, indexed_values, opts \\ []),
    do: watch_by(schema, indexed_values, :updates, opts)

  @doc """
  Equivalent to `watch_by(schema, indexed_values, :changes, opts)`
  """
  def watch_changes_by(schema, indexed_values, opts \\ []),
    do: watch_by(schema, indexed_values, :changes, opts)

  @doc """
  Creates a watch on the provided SchemaMetadata named key.

  ## Arguments

  - `queryable`: The schema to watch, or a query
  - `name`: The name of the watch
  - `opts`: The options for the watch

  ### Name

  - `:inserts`: Signals for each insert or upsert
  - `:deletes`: Signals for each delete
  - `:collection`: Signals for each insert, upsert, or delete
  - `:updates`: Signals for each update (via `Repo.update/*`)
  - `:changes`: Signals for each insert, upsert, delete, or update

  ### Options

  - `:label`: An optional atom label that if provided, will be used by subsequent calls
    to `Repo.assign_ready/3` to store the data into the assigns map. There is no default
    behavior. If a label is not provided, `assign_ready` will fail.
    to `Repo.assign_ready/3` to query the database for data to be stored in the assigns map.
    By default, `Repo.all(schema)` is used.
  """
  def watch(queryable, name, opts \\ []) do
    watch_by(queryable, [], name, opts)
  end

  def watch_by(queryable, indexed_values, name, opts \\ []) do
    {tenant, tx} = assert_tenant_tx!()

    {schema, query_opts} =
      case queryable do
        %Ecto.Query{from: %Ecto.Query.FromExpr{source: {_source, schema}}} ->
          {schema, query: queryable}

        schema ->
          {schema, []}
      end

    source = Schema.get_source(schema)

    idx = tx_lookup_idx!(tenant, source, indexed_values)

    index_name = idx[:id]
    fields = idx[:fields]
    index_values = Indexer.Default.get_index_values(schema, idx[:fields], indexed_values)
    future_ref = :erlfdb.watch(tx, key(tenant, source, index_name, fields, index_values, name))

    opts = Keyword.merge(opts, query_opts)

    Future.new_deferred(
      future_ref,
      fn _ ->
        {schema, {__MODULE__, indexed_values, name}, opts,
         fn _, new_opts ->
           watch_by(schema, indexed_values, name, Keyword.merge(opts, new_opts))
         end}
      end
    )
  end

  defp select_index(metadata, []) do
    %Metadata{indexes: indexes} = with_schema_metadata_indexes(metadata)

    Enum.find(indexes, fn idx ->
      Enum.empty?(idx[:fields])
    end)
  end

  defp select_index(metadata, constraints) do
    Metadata.select_index(with_schema_metadata_indexes(metadata), constraints)
  end

  @doc """
  Asynchronously get the `inserts` key.
  """
  def async_inserts(schema), do: async_get(schema, [], :inserts)

  @doc """
  Asynchronously get the `deletes` key.
  """
  def async_deletes(schema), do: async_get(schema, [], :deletes)

  @doc """
  Asynchronously get the `collection` key.
  """
  def async_collection(schema), do: async_get(schema, [], :collection)

  @doc """
  Asynchronously get the `updates` key.
  """
  def async_updates(schema), do: async_get(schema, [], :updates)

  @doc """
  Asynchronously get the `changes` key.
  """
  def async_changes(schema), do: async_get(schema, [], :changes)

  @doc """
  Asynchronously get the `inserts` key.
  """
  def async_inserts_by(schema, by), do: async_get(schema, by, :inserts)

  @doc """
  Asynchronously get the `deletes` key.
  """
  def async_deletes_by(schema, by), do: async_get(schema, by, :deletes)

  @doc """
  Asynchronously get the `collection` key.
  """
  def async_collection_by(schema, by), do: async_get(schema, by, :collection)

  @doc """
  Asynchronously get the `updates` key.
  """
  def async_updates_by(schema, by), do: async_get(schema, by, :updates)

  @doc """
  Asynchronously get the `changes` key.
  """
  def async_changes_by(schema, by), do: async_get(schema, by, :changes)

  defp sync_get(schema, indexed_values, name, prefix: tenant),
    do:
      Tx.transactional(tenant, fn _ ->
        schema |> async_get(indexed_values, name) |> Future.result()
      end)

  defp async_get(schema, indexed_values, name) do
    source = Schema.get_source(schema)
    future = Future.new()
    {tenant, tx} = assert_tenant_tx!()
    idx = tx_lookup_idx!(tenant, source, indexed_values)
    index_name = idx[:id]
    fields = idx[:fields]
    index_values = Indexer.Default.get_index_values(schema, fields, indexed_values)
    future_ref = :erlfdb.get(tx, key(tenant, source, index_name, fields, index_values, name))
    Future.set(future, tx, future_ref, &decode_counter/1)
  end

  # This defines the full set of values that SchemaMetadata tracks.
  #
  # It might feel natural to add a `:count` field. However, this can't be done
  # accurately due to upserts.
  @doc false
  def signal_names() do
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
    options = Keyword.get(idx, :options, [])
    signals = Keyword.get(options, :signals, signal_names())
    Pack.schema_metadata_ranges(tenant, idx[:source], idx[:id], length(idx[:fields]), signals)
  end

  @impl true
  def create(_tenant, _tx, _idx, _schema, {_start_key, end_key}, _limit) do
    {0, {end_key, end_key}}
  end

  @impl true
  def set(tenant, tx, idx, schema, {_kv_codec, data_object}) do
    index_values = Indexer.Default.get_index_values(schema, idx[:fields], data_object)
    add(tenant, tx, idx, index_values, :inserts)
    add(tenant, tx, idx, index_values, :collection)
    add(tenant, tx, idx, index_values, :changes)
    :ok
  end

  @impl true
  def update(tenant, tx, idx, schema, {_kv_codec, data_object}, updates) do
    old_index_values = Indexer.Default.get_index_values(schema, idx[:fields], data_object)

    data_object =
      data_object
      |> Keyword.merge(updates[:set] || [])
      |> Keyword.drop(updates[:clear] || [])

    case Indexer.Default.get_index_values(schema, idx[:fields], data_object) do
      ^old_index_values ->
        add(tenant, tx, idx, old_index_values, :updates)
        add(tenant, tx, idx, old_index_values, :changes)

      new_index_values ->
        add(tenant, tx, idx, old_index_values, :deletes)
        add(tenant, tx, idx, old_index_values, :collection)
        add(tenant, tx, idx, old_index_values, :updates)
        add(tenant, tx, idx, old_index_values, :changes)

        add(tenant, tx, idx, new_index_values, :inserts)
        add(tenant, tx, idx, new_index_values, :collection)
        add(tenant, tx, idx, new_index_values, :updates)
        add(tenant, tx, idx, new_index_values, :changes)
    end
  end

  @impl true
  def clear(tenant, tx, idx, schema, {_kv_codec, data_object}) do
    index_values = Indexer.Default.get_index_values(schema, idx[:fields], data_object)
    add(tenant, tx, idx, index_values, :deletes)
    add(tenant, tx, idx, index_values, :collection)
    add(tenant, tx, idx, index_values, :changes)
    :ok
  end

  @impl true
  def range(_idx, _plan, _options) do
    raise Unsupported, """
    SchemaMetadata cannot be queried by Ecto.
    """
  end

  defp key(tenant, source, index_name, fields, values, name) do
    Pack.schema_metadata_pack(tenant, source, index_name, length(fields), values, name)
  end

  defp add(tenant, tx, idx, index_values, name, num \\ 1) do
    options = Keyword.get(idx, :options, [])
    signals = Keyword.get(options, :signals, signal_names())
    fields = idx[:fields]

    if name in signals do
      :erlfdb.add(tx, key(tenant, idx[:source], idx[:id], fields, index_values, name), num)
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

  defp with_schema_metadata_indexes(md = %Metadata{indexes: indexes}) do
    indexes =
      Enum.filter(indexes, fn index ->
        index[:indexer] == EctoFoundationDB.Indexer.SchemaMetadata
      end)

    %{md | indexes: indexes}
  end

  defp tx_lookup_idx!(tenant, source, indexed_values) do
    adapter_meta = get_adapter_meta(Tenant.repo(tenant))

    constraints =
      for {f, v} <- indexed_values, do: %QueryPlan.Equal{field: f, is_pk?: false, param: v}

    idx =
      Metadata.transactional(tenant, adapter_meta, source, fn _tx, metadata ->
        select_index(metadata, constraints)
      end)

    if is_nil(idx) do
      raise """
      SchemaMetadata index not found for input constraints:

      #{inspect(indexed_values)}
      """
    end

    idx
  end

  defp get_adapter_meta(repo) do
    {adapter_meta, _} = Ecto.Repo.Supervisor.tuplet(repo, [])
    adapter_meta
  end
end
