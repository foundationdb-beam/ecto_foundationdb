defmodule Ecto.Adapters.FoundationDB.Layer.Indexer.MaxValue do
  @moduledoc """
  From a specified field on a schema, stores the max value.

  This has optimal performance for values where the max is increasing.
  If the max decreases, then the full schema must be scanned to compute
  the new max.

  It is also written to assume that any value is an unsigned integer. A
  value of -1 is returned if there are no values.
  """
  alias Ecto.Adapters.FoundationDB.Exception.Unsupported
  alias Ecto.Adapters.FoundationDB.Layer.Indexer
  alias Ecto.Adapters.FoundationDB.Layer.Pack

  @behaviour Indexer

  def decode(:not_found), do: -1
  def decode(x), do: :binary.decode_unsigned(x, :little)

  def create(tx, idx, _schema) do
    index_name = idx[:id]
    source = idx[:source]
    [max_field] = idx[:fields]

    {key_start, key_end} = Pack.primary_range(source)

    tx
    |> :erlfdb.get_range(key_start, key_end)
    |> :erlfdb.wait()
    |> Enum.each(fn {_fdb_key, fdb_value} ->
      data = Pack.from_fdb_value(fdb_value)
      val = data[max_field]
      :erlfdb.max(tx, key(source, index_name), val)
    end)
  end

  def set(tx, idx, _schema, {_, data}) do
    index_name = idx[:id]
    source = idx[:source]
    [max_field] = idx[:fields]
    val = data[max_field]
    :erlfdb.max(tx, key(source, index_name), val)
  end

  def clear(tx, idx, schema, {_, data}) do
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
      :erlfdb.clear(tx, key)
      create(tx, idx, schema)
    else
      # someone else is the max, so we are free to do nothing
      :ok
    end
  end

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
