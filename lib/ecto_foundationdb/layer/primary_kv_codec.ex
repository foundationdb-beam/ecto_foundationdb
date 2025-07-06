defmodule EctoFoundationDB.Layer.PrimaryKVCodec do
  @moduledoc false
  alias EctoFoundationDB.Layer.InternalMetadata
  alias EctoFoundationDB.Layer.Pack
  alias EctoFoundationDB.Layer.PrimaryKVCodec.StreamDecoder
  alias EctoFoundationDB.Options
  alias EctoFoundationDB.Tenant

  defstruct [:tuple, :vs?, :packed]

  def metadata_key(), do: :multikey

  def new(tuple, vs \\ false)

  def new(tuple, vs) when is_tuple(tuple) do
    %__MODULE__{tuple: tuple, vs?: vs}
  end

  def new(key, vs) when is_binary(key) do
    %__MODULE__{packed: key, vs?: vs}
  end

  def vs?(%__MODULE__{vs?: vs?}), do: vs?

  def stream_decode(kvs, tenant, opts \\ []) do
    StreamDecoder.stream_decode(kvs, tenant, opts)
  end

  def encode(kv_codec, fdb_value, options) do
    max_single_value_size = Options.get(options, :max_single_value_size)
    max_value_size = Options.get(options, :max_value_size)

    size = byte_size(fdb_value)

    if size > max_value_size do
      raise ArgumentError, """
      EctoFoundationDB is configured to reject any objects larger than #{max_value_size} bytes.

      We've encountered a binary of size #{size} bytes.
      """
    end

    fdb_key = pack_key(kv_codec, nil)

    if size > max_single_value_size do
      # split binary, create keys
      crc = :erlang.crc32(fdb_value)

      chunks = binary_chunk_by(fdb_value, max_single_value_size, [])

      n = length(chunks)

      multikey_kvs =
        for(
          {chunk, idx} <- Enum.with_index(chunks),
          do: {pack_key(kv_codec, codec_metadata_tuple(n, idx, crc)), chunk}
        )

      # Write metadata to the DB. This ensures the "primary write key"
      # is always updated (crc), which is required for watches to work as expected.
      meta_fdb_value =
        InternalMetadata.new(metadata_key(), codec_metadata_tuple(n, -1, crc))
        |> Pack.to_fdb_value()

      {true, [{fdb_key, meta_fdb_value} | multikey_kvs]}
    else
      {false, [{fdb_key, fdb_value}]}
    end
  end

  def binary_chunk_by(<<>>, _size, acc), do: Enum.reverse(acc)

  def binary_chunk_by(bin, size, acc) do
    case bin do
      <<chunk::binary-size(size), rest::binary>> ->
        binary_chunk_by(rest, size, [chunk | acc])

      chunk ->
        Enum.reverse([chunk | acc])
    end
  end

  defp codec_metadata_tuple(n, i, crc) do
    {n, i, crc}
  end

  def with_packed_key(kv_codec = %{packed: packed}) when not is_nil(packed), do: kv_codec

  def with_packed_key(kv_codec) do
    %{kv_codec | packed: pack_key(kv_codec, nil)}
  end

  def with_unpacked_tuple(kv_codec = %{tuple: tuple}, _tenant) when not is_nil(tuple),
    do: kv_codec

  def with_unpacked_tuple(kv_codec, tenant) do
    %{kv_codec | tuple: Tenant.unpack(tenant, kv_codec.packed)}
  end

  def pack_key(kv_codec, t) do
    %__MODULE__{vs?: vs?} = kv_codec

    tuple =
      kv_codec.tuple
      |> add_codec_metadata(t)

    if vs?, do: :erlfdb_tuple.pack_vs(tuple), else: :erlfdb_tuple.pack(tuple)
  end

  def set_new_kvs(tx, %__MODULE__{vs?: true}, kvs) do
    for {k, v} <- kvs do
      :erlfdb.set_versionstamped_key(tx, k, v)
    end
  end

  def set_new_kvs(tx, %__MODULE__{vs?: false}, kvs) do
    for {k, v} <- kvs do
      :erlfdb.set(tx, k, v)
    end
  end

  def range(kv_codec) do
    %__MODULE__{vs?: vs?} = kv_codec
    tuple = add_codec_metadata(kv_codec.tuple, nil)
    start_key = if vs?, do: :erlfdb_tuple.pack_vs(tuple), else: :erlfdb_tuple.pack(tuple)
    {start_key, start_key <> <<0xFF>>}
  end

  # add_codec_metadata({:a, :tuple}, nil) -> {:a, :tuple}
  # add_codec_metadata({:a, :tuple}, {0}) -> {:a, :tuple, {0}}
  defp add_codec_metadata(tuple, nil), do: tuple

  defp add_codec_metadata(tuple, t) do
    Tuple.insert_at(tuple, tuple_size(tuple), t)
  end
end
