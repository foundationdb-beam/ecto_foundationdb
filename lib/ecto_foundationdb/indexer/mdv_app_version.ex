defmodule EctoFoundationDB.Indexer.MDVAppVersion do
  @moduledoc false
  # From a specified field on a schema, stores the max value.

  # This index assumes:
  #   * the field value is an unsigned integer
  #   * the max is monotonically non-decreasing

  # A value of -1 is returned if there are no values.
  alias EctoFoundationDB.Exception.Unsupported
  alias EctoFoundationDB.Indexer
  alias EctoFoundationDB.Layer
  alias EctoFoundationDB.Layer.MetadataVersion
  alias EctoFoundationDB.Layer.Pack

  @behaviour Indexer

  @impl true
  def create_range(tenant, idx) do
    source = idx[:source]
    Pack.primary_range(tenant, source)
  end

  @impl true
  def drop_ranges(tenant, idx) do
    [MetadataVersion.app_version_key(tenant, idx)]
  end

  @impl true
  def create(tenant, tx, idx, _schema, {start_key, end_key}, limit) do
    [max_field] = idx[:fields]

    case :erlfdb.get_range(tx, start_key, end_key, limit: limit, wait: true) do
      [] ->
        {0, {end_key, end_key}}

      kvs ->
        max_val =
          kvs
          |> Stream.map(fn {_, fdb_value} ->
            data = Pack.from_fdb_value(fdb_value)
            data[max_field]
          end)
          |> Enum.max()

        update_metadata_version(tenant, tx, idx, max_val)
        {_, last_key} = List.last(kvs)
        next_key = :erlfdb_key.strinc(last_key)

        {length(kvs), {next_key, end_key}}
    end
  end

  @impl true
  def set(tenant, tx, idx, _schema, {_, data}) do
    [max_field] = idx[:fields]
    val = data[max_field]
    update_metadata_version(tenant, tx, idx, val)
  end

  @impl true
  def clear(_tenant, _tx, _idx, _schema, _kv) do
    # Entries from SchemaMigration are never individually cleared
    :ok
  end

  @impl true
  def range(_idx, _plan, _options) do
    raise Unsupported, """
    Using an Ecto Query on an index created with #{__MODULE__} isn't supported.
    """
  end

  defp update_metadata_version(tenant, tx, idx, val) do
    Layer.MetadataVersion.tx_set_app(tenant, tx, idx, val)
    Layer.MetadataVersion.tx_set_global(tx)
  end
end
