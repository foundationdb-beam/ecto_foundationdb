defmodule EctoFoundationDB.Layer.PrimaryKVCodec.StreamDecoder do
  @moduledoc false
  alias EctoFoundationDB.Layer.DecodedKV
  alias EctoFoundationDB.Layer.InternalMetadata
  alias EctoFoundationDB.Layer.Pack
  alias EctoFoundationDB.Layer.PrimaryKVCodec
  alias EctoFoundationDB.Tenant
  alias EctoFoundationDB.Versionstamp

  def stream_decode(kvs, tenant, opts) do
    Stream.transform(
      kvs,
      fn -> stream_decode_start(tenant, opts) end,
      &stream_decode_reducer/2,
      &stream_decode_last/1,
      &stream_decode_after/1
    )
  end

  defp stream_decode_start(tenant, opts) do
    emit_db_key? = Keyword.get(opts, :emit_db_key?, false)

    %{
      db_key: nil,
      key_tuple: nil,
      values: [],
      meta: nil,
      tenant: tenant,
      emit_db_key?: emit_db_key?
    }
  end

  defp stream_decode_reducer(
         {k, v},
         acc = %{meta: nil, tenant: tenant, emit_db_key?: emit_db_key?}
       ) do
    # v is either a standard ecto object or metadata for a multikey object
    # To discover which one, we must convert it from the binary.
    #
    # If this step crashes, it means that we've unexpectedly encountered an individual
    # multikey key-value without having previously found the metadata to guide us to decode it
    v = Pack.from_fdb_value(v)

    metadata_key = PrimaryKVCodec.metadata_key()

    case InternalMetadata.fetch(v) do
      {:ok, {^metadata_key, meta}} ->
        # Found a multikey object, so we start processing it
        key_tuple = Tenant.unpack(tenant, k)
        {[], %{acc | db_key: k, key_tuple: key_tuple, values: [], meta: meta}}

      {:ok, metadata} ->
        raise ArgumentError, """
        EctoFoundationDB encountered metadata #{metadata}. We don't know how to process this.

        Data: #{inspect(v)}
        """

      :error ->
        key_tuple = Tenant.unpack(tenant, k)
        data_object = extract_complete_vs(key_tuple, v)

        item = %DecodedKV{
          codec: Pack.primary_write_key_to_codec(tenant, k),
          data_object: data_object
        }

        {emit(emit_db_key?, k, item), %{acc | db_key: nil, key_tuple: nil, values: [], meta: nil}}
    end
  end

  defp stream_decode_reducer(
         {k, v},
         acc = %{
           db_key: orig_key,
           key_tuple: key_tuple,
           values: values,
           meta: {n, i, crc},
           tenant: tenant,
           emit_db_key?: emit_db_key?
         }
       ) do
    split_key_tuple = Tenant.unpack(tenant, k)

    case parse_codec_metadata_tuple(split_key_tuple) do
      {true, {^n, i2, ^crc}} when i2 == i + 1 and n == i2 + 1 ->
        fdb_value = :erlang.iolist_to_binary(Enum.reverse([v | values]))

        case :erlang.crc32(fdb_value) do
          ^crc ->
            data_object = Pack.from_fdb_value(fdb_value)
            data_object = extract_complete_vs(key_tuple, data_object)

            item =
              %DecodedKV{
                codec: Pack.primary_write_key_to_codec(tenant, key_tuple),
                data_object: data_object,
                multikey?: true
              }

            {emit(emit_db_key?, orig_key, item), %{acc | key_tuple: nil, values: [], meta: nil}}

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

  defp stream_decode_last(acc), do: {[], acc}

  defp stream_decode_after(_acc), do: :ok

  defp extract_complete_vs(key_tuple, data_object) do
    [{pk_field, stored_pk} | data_object_rest] = data_object

    # When incomplete versionstamp is stored on in the value, we need to retrieve the pk from the key
    if Versionstamp.incomplete?(stored_pk) do
      [{pk_field, Pack.get_vs_from_key_tuple(key_tuple)} | data_object_rest]
    else
      data_object
    end
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

  defp emit(false, _key, item), do: [item]
  defp emit(true, key, item), do: [{key, item}]
end
