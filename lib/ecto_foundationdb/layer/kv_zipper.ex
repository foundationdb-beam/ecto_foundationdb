defmodule EctoFoundationDB.Layer.KVZipper do
  alias EctoFoundationDB.Future
  alias EctoFoundationDB.Layer.InternalMetadata
  alias EctoFoundationDB.Layer.Pack
  alias EctoFoundationDB.Options
  alias EctoFoundationDB.Tenant

  defstruct [:tuple]

  @magic_key :unzipped

  def new(tuple) do
    %__MODULE__{tuple: tuple}
  end

  def stream_zip(kvs, tenant) do
    Stream.transform(
      kvs,
      %{key_tuple: nil, values: [], meta: nil, tenant: tenant},
      &stream_zip_reducer/2
    )
  end

  def unzip(zipper, fdb_value, options) do
    max_single_value_size = Options.get(options, :max_single_value_size)
    max_value_size = Options.get(options, :max_value_size)

    size = byte_size(fdb_value)

    if size > max_value_size do
      raise ArgumentError, """
      EctoFoundationDB is configured to reject any objects larger than #{max_value_size} bytes.

      We've encountered a binary of size #{size} bytes.
      """
    end

    fdb_key = pack_key(zipper, nil)

    if size > max_single_value_size do
      # split binary, create keys
      crc = :erlang.crc32(fdb_value)

      chunks = binary_chunk_by(fdb_value, max_single_value_size, [])

      n = length(chunks)

      unzipped_kvs =
        for(
          {chunk, idx} <- Enum.with_index(chunks),
          do: {pack_key(zipper, zipper_metadata_tuple(n, idx, crc)), chunk}
        )

      # Write metadata to the DB. This ensures the "primary write key"
      # is always updated (crc), which is required for watches to work as expected.
      meta_fdb_value =
        InternalMetadata.new(@magic_key)
        |> Keyword.merge(meta: zipper_metadata_tuple(n, -1, crc))
        |> Pack.to_fdb_value()

      {true, [{fdb_key, meta_fdb_value} | unzipped_kvs]}
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

  defp zipper_metadata_tuple(n, i, crc) do
    {n, i, crc}
  end

  defp parse_zipper_metadata_tuple(key_tuple) do
    last_element = elem(key_tuple, tuple_size(key_tuple) - 1)

    case last_element do
      meta = {_n, _i, _crc} ->
        # the only nested tuple we use is for this KVZipper, so if the last
        # element is a tuple, we know it's our tuple
        {true, meta}

      _ ->
        false
    end
  end

  def pack_key(zipper, t) do
    zipper.tuple
    |> add_zip_metadata(t)
    |> :erlfdb_tuple.pack()
  end

  def range(zipper) do
    tuple = add_zip_metadata(zipper.tuple, nil)
    start_key = :erlfdb_tuple.pack(tuple)
    {start_key, start_key <> <<0xFF>>}
  end

  def async_get(zipper, tenant, tx, future) do
    {start_key, end_key} = range(zipper)
    future_ref = :erlfdb.get_range(tx, start_key, end_key, wait: false)

    # Same API contract as :erlfdb.get except the value will already be decoded with
    # Pack.from_fdb_value
    f = fn
      [] ->
        :not_found

      [{_k, v}] ->
        Pack.from_fdb_value(v)

      kvs ->
        [{_k, v}] =
          kvs
          |> stream_zip(tenant)
          |> Enum.to_list()

        v
    end

    Future.set(future, tx, future_ref, f)
  end

  # add_zip_metadata({:a, :tuple}, nil) -> {:a, :tuple}
  # add_zip_metadata({:a, :tuple}, {0}) -> {:a, :tuple, {0}}
  defp add_zip_metadata(tuple, nil), do: tuple

  defp add_zip_metadata(tuple, t) do
    Tuple.insert_at(tuple, tuple_size(tuple), t)
  end

  defp stream_zip_reducer({k, v}, acc = %{meta: nil, tenant: tenant}) do
    # v is either a standard ecto object or metadata for an unzipped object
    # To discover which one, we must convert it from the binary.
    #
    # If this step crashes, it means that we've unexpectedly encountered an individual
    # unzipped key-value without having previously found the metadata to guide us to zip it
    v = Pack.from_fdb_value(v)

    case InternalMetadata.fetch(v) do
      {true, @magic_key} ->
        # Found an unzipped object, so we start processing it
        key_tuple = Tenant.unpack(tenant, k)
        meta = Keyword.fetch!(v, :meta)
        {[], %{acc | key_tuple: key_tuple, values: [], meta: meta}}

      {true, module} ->
        raise ArgumentError, """
        EctoFoundationDB encountered metadata from #{module}. We don't know how to process this.

        Data: #{inspect(v)}
        """

      {false, nil} ->
        item = {Pack.primary_write_key_to_zipper(tenant, k), v}
        {[item], %{acc | key_tuple: nil, values: [], meta: nil}}
    end
  end

  defp stream_zip_reducer(
         {k, v},
         acc = %{key_tuple: key_tuple, values: values, meta: {n, i, crc}, tenant: tenant}
       ) do
    split_key_tuple = Tenant.unpack(tenant, k)

    case parse_zipper_metadata_tuple(split_key_tuple) do
      {true, {^n, i2, ^crc}} when i2 == i + 1 and n == i2 + 1 ->
        fdb_value = :erlang.iolist_to_binary(Enum.reverse([v | values]))

        case :erlang.crc32(fdb_value) do
          ^crc ->
            item =
              {Pack.primary_write_key_to_zipper(tenant, key_tuple),
               Pack.from_fdb_value(fdb_value)}

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
