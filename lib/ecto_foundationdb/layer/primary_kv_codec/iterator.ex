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
    :limit,

    # True if key-values are provided in reverse order
    :backward?
  ]
  defstruct @enforce_keys

  def start(nil, kvs, tenant, opts) do
    state = %__MODULE__{
      kvs: kvs,
      tenant: tenant,
      buffer: [],
      key_tuple: nil,
      meta: nil,
      limit: Keyword.get(opts, :limit, nil),
      backward?: Keyword.get(opts, :backward?, false)
    }

    :erlfdb_iterator.new(__MODULE__, state)
  end

  def start(cont_state = %__MODULE__{}, more_kvs, tenant, opts) do
    %{
      kvs: kvs,
      buffer: buffer,
      meta: meta,
      key_tuple: key_tuple,
      backward?: backward?
    } = cont_state

    state = %__MODULE__{
      kvs: kvs ++ more_kvs,
      tenant: tenant,
      buffer: buffer,
      key_tuple: key_tuple,
      meta: meta,
      limit: Keyword.get(opts, :limit, nil),
      backward?: backward?
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

  # No multikey object in progress - looking for either a single-key object or start of multikey
  def handle_next(state = %__MODULE__{kvs: [kv | kvs], meta: nil}) do
    {k, v} = kv
    %{tenant: tenant, limit: limit, backward?: backward?} = state

    key_tuple = Tenant.unpack(tenant, k)

    case detect_multikey_start(key_tuple, v, backward?) do
      {:single, data_object} ->
        # Single-key object
        data_object = extract_complete_vs(key_tuple, data_object)

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

      {:multikey_start, base_key_tuple, meta} ->
        # Start of a multikey object
        {:cont, [], %{state | kvs: kvs, buffer: [kv], key_tuple: base_key_tuple, meta: meta}}
    end
  end

  # Multikey object in progress - processing chunks
  def handle_next(state = %__MODULE__{kvs: [kv | kvs]}) do
    {k, _v} = kv

    %{
      buffer: buffer,
      key_tuple: key_tuple,
      meta: {n, i, crc},
      tenant: tenant,
      limit: limit,
      backward?: backward?
    } = state

    split_key_tuple = Tenant.unpack(tenant, k)

    case check_multikey_progress(split_key_tuple, key_tuple, {n, i, crc}, backward?) do
      {:continue, new_meta} ->
        # Continue buffering chunks
        {:cont, [], %{state | kvs: kvs, buffer: [kv | buffer], meta: new_meta}}

      :complete ->
        # All chunks collected, emit the object
        {metadata_key, all_keys, values} = extract_multikey_parts(kv, buffer, backward?)

        fdb_value = :erlang.iolist_to_binary(values)

        case :erlang.crc32(fdb_value) do
          ^crc ->
            data_object = Pack.from_fdb_value(fdb_value)
            data_object = extract_complete_vs(key_tuple, data_object)

            first_key = metadata_key
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

      {:error, other} ->
        direction = if backward?, do: "(backward)", else: ""

        raise """
        Metadata error #{direction}. Previous: #{inspect({n, i, crc})}, Encountered: #{inspect(other)}
        """
    end
  end

  @impl true
  def handle_stop(_state = %__MODULE__{}) do
    :ok
  end

  # Detect if this KV starts a multikey object or is a single-key object
  # Forward: metadata header has InternalMetadata in value
  # Backward: last chunk has codec metadata tuple in key
  defp detect_multikey_start(key_tuple, v, _backward? = false) do
    metadata_key = PrimaryKVCodec.metadata_key()
    v = Pack.from_fdb_value(v)

    case InternalMetadata.fetch(v) do
      {:ok, {^metadata_key, meta}} ->
        {:multikey_start, key_tuple, meta}

      :error ->
        {:single, v}

      {:ok, metadata} ->
        raise ArgumentError, """
        EctoFoundationDB encountered metadata #{inspect(metadata)}. We don't know how to process this.

        Data: #{inspect(v)}
        """
    end
  end

  defp detect_multikey_start(key_tuple, v, _backward? = true) do
    case parse_codec_metadata_tuple(key_tuple) do
      {true, meta} ->
        # Found the last chunk of a multikey object
        # Extract the base key_tuple (without the codec metadata suffix)
        base_key_tuple = Tuple.delete_at(key_tuple, tuple_size(key_tuple) - 1)
        {:multikey_start, base_key_tuple, meta}

      false ->
        # Single-key object
        {:single, Pack.from_fdb_value(v)}
    end
  end

  # Check if we should continue buffering or if multikey object is complete
  # Forward: indices increment from 0 to n-1
  # Backward: indices decrement from n-1 to 0, then metadata header
  defp check_multikey_progress(split_key_tuple, _key_tuple, {n, i, crc}, _backward? = false) do
    case parse_codec_metadata_tuple(split_key_tuple) do
      {true, {^n, i2, ^crc}} when i2 == i + 1 and n == i2 + 1 ->
        :complete

      {true, {^n, i2, ^crc}} when i2 == i + 1 and i2 < n ->
        {:continue, {n, i2, crc}}

      other ->
        {:error, other}
    end
  end

  defp check_multikey_progress(split_key_tuple, key_tuple, {n, i, crc}, _backward? = true) do
    case parse_codec_metadata_tuple(split_key_tuple) do
      {true, {^n, i2, ^crc}} when i2 == i - 1 and i2 >= 0 ->
        {:continue, {n, i2, crc}}

      false when i == 0 and split_key_tuple == key_tuple ->
        :complete

      other ->
        {:error, other}
    end
  end

  # Extract metadata key, all chunk keys, and values from buffer
  # Forward: current_kv is the last chunk, buffer has [metadata_kv | earlier_chunks] in reverse order
  # Backward: current_kv is metadata header, buffer has chunks in forward order
  defp extract_multikey_parts(current_kv, buffer, _backward? = false) do
    # Prepend current kv (last chunk) to buffer, then reverse to get forward order
    # Skip metadata value (first after reversing)
    {all_keys, [_ | values]} =
      [current_kv | buffer]
      |> Enum.reverse()
      |> Enum.unzip()

    metadata_key = hd(all_keys)
    {metadata_key, all_keys, values}
  end

  defp extract_multikey_parts({metadata_key, _metadata_value}, buffer, _backward? = true) do
    # Buffer is prepended during backward iteration, so chunks are already in forward order
    {all_keys, values} = Enum.unzip(buffer)
    {metadata_key, all_keys, values}
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
