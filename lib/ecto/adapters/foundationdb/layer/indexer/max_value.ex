defmodule Ecto.Adapters.FoundationDB.Layer.Indexer.MaxValue do
  @moduledoc false
  alias Ecto.Adapters.FoundationDB.Exception.Unsupported
  alias Ecto.Adapters.FoundationDB.Layer.Indexer
  alias Ecto.Adapters.FoundationDB.Layer.Pack

  @behaviour Indexer

  def decode(:not_found), do: -1
  def decode(x), do: :binary.decode_unsigned(x, :little)

  def create(tx, idx, _adapter_meta) do
    index_name = idx[:id]
    source = idx[:source]
    [max_field] = idx[:fields]

    {key_start, key_end} = Pack.primary_range(source)

    tx
    |> :erlfdb.get_range(key_start, key_end)
    |> :erlfdb.wait()
    |> Enum.each(fn {_fdb_key, fdb_value} ->
      data = Pack.from_fdb_value(fdb_value)
      key = key(source, index_name)
      val = data[max_field]
      :erlfdb.max(tx, key, val)
    end)
  end

  def set(tx, idx, _adapter_meta, {_, data}) do
    index_name = idx[:id]
    source = idx[:source]
    [max_field] = idx[:fields]
    key = key(source, index_name)
    val = data[max_field]
    :erlfdb.max(tx, key, val)
  end

  def clear(tx, idx, adapter_meta, {_, data}) do
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
      create(tx, idx, adapter_meta)
    else
      # someone else is the max, so we are free to do nothing
      :ok
    end
  end

  def range(_idx, _adapter_meta, _plan, _options) do
    raise Unsupported, """
    Using an Ecto Query on an index created with #{__MODULE__} isn't supported.
    """
  end

  def key(source, index_name) do
    Pack.namespaced_pack(source, "max", ["#{index_name}"])
  end
end
