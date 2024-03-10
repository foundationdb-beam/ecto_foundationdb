defmodule Ecto.Adapters.FoundationDB.Layer.Pack do
  @moduledoc """
  This internal module creates binaries to be used as FDB keys and values.
  """
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

  ## Examples

    iex> Ecto.Adapters.FoundationDB.Layer.Pack.indexkey_encoder("an-example-key")
    <<37, 99, 56, 165>>

    iex> Ecto.Adapters.FoundationDB.Layer.Pack.indexkey_encoder(~N[2024-03-01 12:34:56], timeseries: true)
    "2024-03-01T12:34:56"

  """
  def indexkey_encoder(x, index_options \\ []) do
    indexkey_encoder(x, 4, index_options)
  end

  @doc """
  ## Examples

    iex> Ecto.Adapters.FoundationDB.Layer.Pack.indexkey_encoder("an-example-key", 1, [])
    <<102>>

  """
  def indexkey_encoder(x, num_bytes, index_options) do
    if index_options[:timeseries] do
      NaiveDateTime.to_iso8601(x)
    else
      <<n::unsigned-big-integer-size(num_bytes * 8)>> =
        <<-1::unsigned-big-integer-size(num_bytes * 8)>>

      i = :erlang.phash2(x, n)
      <<i::unsigned-big-integer-size(num_bytes * 8)>>
    end
  end

  @doc """
  ## Examples

    iex> Ecto.Adapters.FoundationDB.Layer.Pack.to_fdb_indexkey([], [], "table", "my_index", ["abc", "123"], "my-pk-id")
    "table/i/my_index/M\\xDA\\xD8\\xFB/V\\xE3\\xD1\\x01/\\x83m\\0\\0\\0\\bmy-pk-id"

  """
  def to_fdb_indexkey(adapter_opts, index_options, source, index_name, vals, id)
      when is_list(vals) do
    fun = Options.get(adapter_opts, :indexkey_encoder)
    vals = for v <- vals, do: fun.(v, index_options)

    to_raw_fdb_key(
      adapter_opts,
      [source, @index_namespace, index_name | vals] ++
        if(is_nil(id), do: [], else: [encode_pk_for_key(id)])
    )
  end

  @doc """
  ## Examples

    iex> Ecto.Adapters.FoundationDB.Layer.Pack.add_delimiter("my-key", [])
    "my-key/"

    iex> Ecto.Adapters.FoundationDB.Layer.Pack.add_delimiter("my-key", [key_delimiter: <<0>>])
    "my-key\\0"

  """
  def add_delimiter(key, adapter_opts) do
    key <> Options.get(adapter_opts, :key_delimiter)
  end

  @doc """
  This function computes a key for use in FDB to store the data object via the primary key.

  Ideally we could leave strings in the key directly so they are more easily human-readable.
  However, if two distinct primary keys encode to the same datakey from the output of this function,
  then the objects can be put into an inconsistent state.

  For this reason, the only safe approach is to encode all terms with the same mechanism.
  We choose `:erlang.term_to_binary()` for this encoding.

  ## Examples

    iex> Ecto.Adapters.FoundationDB.Layer.Pack.to_fdb_datakey([], "table", "my-pk-id")
    "table/d/\\x83m\\0\\0\\0\\bmy-pk-id"

    iex> Ecto.Adapters.FoundationDB.Layer.Pack.to_fdb_datakey([], "table", :"my-pk-id")
    "table/d/\\x83w\\bmy-pk-id"

    iex> Ecto.Adapters.FoundationDB.Layer.Pack.to_fdb_datakey([], "table", {:tuple, :term})
    "table/d/\\x83h\\x02w\\x05tuplew\\x04term"

  """
  def to_fdb_datakey(adapter_opts, source, x) do
    to_raw_fdb_key(adapter_opts, [source, @data_namespace, encode_pk_for_key(x)])
  end

  @doc """
  ## Examples

    iex> Ecto.Adapters.FoundationDB.Layer.Pack.to_fdb_datakey_startswith([], "table")
    "table/d/"

  """
  def to_fdb_datakey_startswith(adapter_opts, source) do
    to_raw_fdb_key(adapter_opts, [source, @data_namespace, ""])
  end

  @doc """
  ## Examples

    iex> Ecto.Adapters.FoundationDB.Layer.Pack.to_raw_fdb_key([], ["table", "namespace", "id"])
    "table/namespace/id"

    iex> Ecto.Adapters.FoundationDB.Layer.Pack.to_raw_fdb_key([key_delimiter: <<0>>], ["table", "namespace", "id"])
    "table\\0namespace\\0id"

  """
  def to_raw_fdb_key(adapter_opts, list) when is_list(list) do
    Enum.join(list, Options.get(adapter_opts, :key_delimiter))
  end

  @doc """
  ## Examples

    iex> bin = Ecto.Adapters.FoundationDB.Layer.Pack.to_fdb_value([id: "my-pk-id", x: "a-value", y: 42])
    iex> Ecto.Adapters.FoundationDB.Layer.Pack.from_fdb_value(bin)
    [id: "my-pk-id", x: "a-value", y: 42]

  """
  def to_fdb_value(fields), do: :erlang.term_to_binary(fields)

  def from_fdb_value(bin), do: :erlang.binary_to_term(bin)

  def new_index_object(fdb_key, data_object) do
    [
      id: fdb_key,
      data: data_object
    ]
  end

  #
  defp encode_pk_for_key(id), do: :erlang.term_to_binary(id)
end
