defmodule EctoFoundationDB.Layer.PrimaryKVCodec.Iterator do
  @behaviour :erlfdb_iterator

  @moduledoc false
  alias EctoFoundationDB.Layer.DecodedKV
  alias EctoFoundationDB.Layer.InternalMetadata
  alias EctoFoundationDB.Layer.Pack
  alias EctoFoundationDB.Layer.PrimaryKVCodec
  alias EctoFoundationDB.Tenant
  alias EctoFoundationDB.Versionstamp

  @enforce_keys [
    # The input key-values from the database
    :kvs,

    # The tenant
    :tenant,

    # The complete list of key-values from the input for the object
    # being processed in the stream at any given moment. When the
    # stream finishes, if non-empty, then a multikey object is unfinished.
    # Includes the metadata key
    :buffer,

    # The unpacked tuple of the metadata key for the object being decoded.
    :key_tuple,

    # The decoded metadata for the multikey object
    :meta,

    # The maximum number of item to emit
    :limit
  ]
  defstruct @enforce_keys

  def start(nil, kvs, tenant, opts) do
    state = %__MODULE__{
      kvs: kvs,
      tenant: tenant,
      buffer: [],
      key_tuple: nil,
      meta: nil,
      limit: Keyword.get(opts, :limit, nil)
    }

    :erlfdb_iterator.new(__MODULE__, state)
  end

  def start(cont_state = %__MODULE__{}, more_kvs, tenant, opts) do
    %{
      kvs: kvs,
      buffer: buffer,
      meta: meta,
      key_tuple: key_tuple
    } = cont_state

    state = %__MODULE__{
      kvs: kvs ++ more_kvs,
      tenant: tenant,
      buffer: buffer,
      key_tuple: key_tuple,
      meta: meta,
      limit: Keyword.get(opts, :limit, nil)
    }

    :erlfdb_iterator.new(__MODULE__, state)
  end

  def get_state(iterator) do
    {state, _} = :erlfdb_iterator.call(iterator, :get_state)
    state
  end

  @impl true
  def handle_call(:get_state, _, state = %__MODULE__{}) do
    {:reply, state, state}
  end

  @impl true
  def handle_next(state = %__MODULE__{kvs: []}), do: {:halt, state}

  def handle_next(state = %__MODULE__{kvs: [kv | kvs], meta: nil}) do
    {k, v} = kv
    %{tenant: tenant, limit: limit} = state

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
        {:cont, [], %{state | kvs: kvs, buffer: [kv], key_tuple: key_tuple, meta: meta}}

      {:ok, metadata} ->
        raise ArgumentError, """
        EctoFoundationDB encountered metadata #{inspect(metadata)}. We don't know how to process this.

        Data: #{inspect(v)}
        """

      :error ->
        key_tuple = Tenant.unpack(tenant, k)
        data_object = extract_complete_vs(key_tuple, v)

        item = %DecodedKV{
          codec: Pack.primary_write_key_to_codec(tenant, k),
          data_object: data_object,
          multikey?: false,
          range: {k, :erlfdb_key.strinc(k)}
        }

        emit(item, %{
          state
          | kvs: kvs,
            buffer: [],
            key_tuple: nil,
            meta: nil,
            limit: decr_limit(limit, 1)
        })
    end
  end

  def handle_next(state = %__MODULE__{kvs: [kv | kvs]}) do
    {k, _v} = kv

    %{
      buffer: buffer,
      key_tuple: key_tuple,
      meta: {n, i, crc},
      tenant: tenant,
      limit: limit
    } = state

    split_key_tuple = Tenant.unpack(tenant, k)

    # Note: ignore the metadata value
    {all_keys, [_ | values]} =
      [kv | buffer]
      |> Enum.reverse()
      |> Enum.unzip()

    case parse_codec_metadata_tuple(split_key_tuple) do
      {true, {^n, i2, ^crc}} when i2 == i + 1 and n == i2 + 1 ->
        fdb_value = :erlang.iolist_to_binary(values)

        case :erlang.crc32(fdb_value) do
          ^crc ->
            data_object = Pack.from_fdb_value(fdb_value)
            data_object = extract_complete_vs(key_tuple, data_object)

            first_key = hd(all_keys)
            last_key = List.last(all_keys)

            item =
              %DecodedKV{
                codec: Pack.primary_write_key_to_codec(tenant, key_tuple),
                data_object: data_object,
                multikey?: true,
                range: {first_key, :erlfdb_key.strinc(last_key)}
              }

            emit(item, %{
              state
              | kvs: kvs,
                buffer: [],
                key_tuple: nil,
                meta: nil,
                limit: decr_limit(limit, 1)
            })

          other_crc ->
            raise """
            Metadata error. Encountered: CRC #{other_crc}, Expected: CRC #{crc},
            """
        end

      {true, meta = {^n, i2, ^crc}} when i2 == i + 1 and i2 < n ->
        {:cont, [], %{state | kvs: kvs, buffer: [kv | buffer], meta: meta}}

      other ->
        raise """
        Metadata error. Previous: #{inspect({n, i, crc})}, Encountered: #{inspect(other)}
        """
    end
  end

  @impl true
  def handle_stop(_state = %__MODULE__{}) do
    :ok
  end

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

  defp emit(item, state), do: {cont_or_halt(state), [item], state}

  defp decr_limit(nil, _by), do: nil
  defp decr_limit(limit, by), do: limit - by

  defp cont_or_halt(%{limit: 0}), do: :halt
  defp cont_or_halt(%{limit: limit}) when limit < 0, do: raise("Invalid limit valuen encountered")
  defp cont_or_halt(%{kvs: []}), do: :halt
  defp cont_or_halt(_), do: :cont
end
