defmodule EctoFoundationDB.Indexer.MaxValue do
  @moduledoc false
  # From a specified field on a schema, stores the max value.

  # This index assumes:
  #   * the field value is an unsigned integer
  #   * the max is monotonically non-decreasing

  # A value of -1 is returned if there are no values.
  alias EctoFoundationDB.Exception.Unsupported
  alias EctoFoundationDB.Indexer
  alias EctoFoundationDB.Layer.Pack

  @behaviour Indexer

  def decode(:not_found), do: -1
  def decode(x), do: :binary.decode_unsigned(x, :little)

  @impl true
  def create_range(idx) do
    source = idx[:source]
    Pack.primary_range(source)
  end

  @impl true
  def create(tx, idx, _schema, {start_key, end_key}, limit) do
    index_name = idx[:id]
    source = idx[:source]
    [max_field] = idx[:fields]

    keys =
      tx
      |> :erlfdb.get_range(start_key, end_key, limit: limit)
      |> :erlfdb.wait()
      |> Enum.map(fn {fdb_key, fdb_value} ->
        data = Pack.from_fdb_value(fdb_value)
        val = data[max_field]
        :erlfdb.max(tx, key(source, index_name), val)
        fdb_key
      end)

    {length(keys), {List.last(keys), end_key}}
  end

  @impl true
  def set(tx, idx, _schema, {_, data}) do
    index_name = idx[:id]
    source = idx[:source]
    [max_field] = idx[:fields]
    val = data[max_field]
    :erlfdb.max(tx, key(source, index_name), val)
  end

  @impl true
  def clear(tx, idx, _schema, {_, data}) do
    index_name = idx[:id]
    source = idx[:source]
    [max_field] = idx[:fields]
    val = data[max_field]
    key = key(source, index_name)

    db_val =
      tx
      |> :erlfdb.get(key)
      |> :erlfdb.wait()
      |> decode()

    if val == db_val do
      # expensive
      raise Unsupported, """
      MaxValue decrease not supported.
      """
    else
      # someone else is the max, so we are free to do nothing
      :ok
    end
  end

  @impl true
  def range(_idx, _plan, _options) do
    raise Unsupported, """
    Using an Ecto Query on an index created with #{__MODULE__} isn't supported.
    """
  end

  def get(tx, source, index_name) do
    :erlfdb.get(tx, key(source, index_name))
  end

  def key(source, index_name) do
    Pack.namespaced_pack(source, "max", ["#{index_name}"])
  end
end
