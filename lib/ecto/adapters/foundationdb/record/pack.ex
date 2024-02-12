defmodule Ecto.Adapters.FoundationDB.Record.Pack do
  alias Ecto.Adapters.FoundationDB.Options

  @data_namespace "d"
  @index_namespace "i"

  @doc """
  In the index key, values must be encoded into a fixed-length binary.

  Fixed-length is required so that get_range can be used reliably in the presence of
  arbitrary data. In a naive approach, the key_delimiter can conflict with
  the bytes included in the index value.

  However, this means our indexes will have conflicts that must be resolved with
  filtering.
  """
  def indexkey_encoder(x) do
    i = :erlang.phash2(x, 0xFFFFFFFF)
    <<i::unsigned-big-integer-size(32)>>
  end

  def to_fdb_indexkey(adapter_opts, source, index_name, vals) when is_list(vals) do
    fun = Options.get(adapter_opts, :indexkey_encoder)
    vals = for v <- vals, do: fun.(v)
    to_raw_fdb_key(adapter_opts, [source, @index_namespace, index_name | vals])
  end

  def add_delimiter(key, adapter_opts) do
    key <> Options.get(adapter_opts, :key_delimiter)
  end

  def to_fdb_datakey(adapter_opts, source, x) do
    to_raw_fdb_key(adapter_opts, [source, @data_namespace, val_for_key(x)])
  end

  def to_fdb_datakey_startswith(adapter_opts, source) do
    to_raw_fdb_key(adapter_opts, [source, @data_namespace, ""])
  end

  def to_raw_fdb_key(adapter_opts, list) when is_list(list) do
    Enum.join(list, Options.get(adapter_opts, :key_delimiter))
  end

  def to_fdb_value(fields), do: :erlang.term_to_binary(fields)

  def from_fdb_value(bin), do: :erlang.binary_to_term(bin)

  def new_index_object(source, fdb_key, pk_field, pk_value, index_entries, value) do
    [
      pk: {pk_field, pk_value},
      value: value,
      index: index_entries,
      source: source,
      full_key: fdb_key
    ]
  end

  defp val_for_key(x) when is_binary(x), do: x
  defp val_for_key(x) when is_integer(x), do: <<x::unsigned-big-integer-size(64)>>
  defp val_for_key(x) when is_atom(x), do: "#{x}"
  defp val_for_key(x), do: :erlang.term_to_binary(x)
end
