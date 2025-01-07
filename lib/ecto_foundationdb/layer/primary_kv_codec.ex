defmodule EctoFoundationDB.Layer.PrimaryKVCodec do
  alias EctoFoundationDB.Layer.DecodedKV
  alias EctoFoundationDB.Layer.InternalMetadata
  alias EctoFoundationDB.Layer.Pack
  alias EctoFoundationDB.Options
  alias EctoFoundationDB.Tenant

  defstruct [:tuple]

  @magic_key :multikey

  def new(tuple) do
    %__MODULE__{tuple: tuple}
  end

  def stream_decode(kvs, tenant) do
    Stream.transform(
      kvs,
      %{key_tuple: nil, values: [], meta: nil, tenant: tenant},
      &stream_decode_reducer/2
    )
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
        InternalMetadata.new(@magic_key)
        |> Keyword.merge(meta: codec_metadata_tuple(n, -1, crc))
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

  defp parse_codec_metadata_tuple(key_tuple) do
    last_element = elem(key_tuple, tuple_size(key_tuple) - 1)

    case last_element do
      meta = {_n, _i, _crc} ->
        # the only nested tuple we use is for this codec, so if the last
        # element is a tuple, we know it's our tuple
        {true, meta}

      _ ->
        false
    end
  end

  def pack_key(kv_codec, t) do
    kv_codec.tuple
    |> add_codec_metadata(t)
    |> :erlfdb_tuple.pack()
  end

  def range(kv_codec) do
    tuple = add_codec_metadata(kv_codec.tuple, nil)
    start_key = :erlfdb_tuple.pack(tuple)
    {start_key, start_key <> <<0xFF>>}
  end

  # add_codec_metadata({:a, :tuple}, nil) -> {:a, :tuple}
  # add_codec_metadata({:a, :tuple}, {0}) -> {:a, :tuple, {0}}
  defp add_codec_metadata(tuple, nil), do: tuple

  defp add_codec_metadata(tuple, t) do
    Tuple.insert_at(tuple, tuple_size(tuple), t)
  end

  defp stream_decode_reducer({k, v}, acc = %{meta: nil, tenant: tenant}) do
    # v is either a standard ecto object or metadata for a multikey object
    # To discover which one, we must convert it from the binary.
    #
    # If this step crashes, it means that we've unexpectedly encountered an individual
    # multikey key-value without having previously found the metadata to guide us to decode it
    v = Pack.from_fdb_value(v)

    case InternalMetadata.fetch(v) do
      {true, @magic_key} ->
        # Found a multikey object, so we start processing it
        key_tuple = Tenant.unpack(tenant, k)
        meta = Keyword.fetch!(v, :meta)
        {[], %{acc | key_tuple: key_tuple, values: [], meta: meta}}

      {true, module} ->
        raise ArgumentError, """
        EctoFoundationDB encountered metadata from #{module}. We don't know how to process this.

        Data: #{inspect(v)}
        """

      {false, nil} ->
        item = %DecodedKV{codec: Pack.primary_write_key_to_codec(tenant, k), data_object: v}
        {[item], %{acc | key_tuple: nil, values: [], meta: nil}}
    end
  end

  defp stream_decode_reducer(
         {k, v},
         acc = %{key_tuple: key_tuple, values: values, meta: {n, i, crc}, tenant: tenant}
       ) do
    split_key_tuple = Tenant.unpack(tenant, k)

    case parse_codec_metadata_tuple(split_key_tuple) do
      {true, {^n, i2, ^crc}} when i2 == i + 1 and n == i2 + 1 ->
        fdb_value = :erlang.iolist_to_binary(Enum.reverse([v | values]))

        case :erlang.crc32(fdb_value) do
          ^crc ->
            data_object = Pack.from_fdb_value(fdb_value)

            item =
              %DecodedKV{
                codec: Pack.primary_write_key_to_codec(tenant, key_tuple),
                data_object: data_object,
                multikey?: true
              }

            {[item], %{acc | key_tuple: nil, values: [], meta: nil}}

          other_crc ->
            raise """
            Metadata error. Encountered: CRC #{other_crc}, Expected: CRC #{crc},
            """
        end

      {true, meta = {^n, i2, ^crc}} when i2 == i + 1 and i2 < n ->
        {[], %{acc | values: [v | values], meta: meta}}

      other ->
        raise """
        Metadata error. Previous: #{inspect({n, i, crc})}, Encountered: #{inspect(other)}
        """
    end
  end
end
