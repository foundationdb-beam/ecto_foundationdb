defmodule EctoFoundationDB.Layer.TxInsert do
  @moduledoc false
  alias EctoFoundationDB.Exception.Unsupported
  alias EctoFoundationDB.Future
  alias EctoFoundationDB.Indexer
  alias EctoFoundationDB.Layer.DecodedKV
  alias EctoFoundationDB.Layer.Pack
  alias EctoFoundationDB.Layer.PrimaryKVCodec
  alias EctoFoundationDB.Layer.Tx

  defstruct [:tenant, :schema, :source, :metadata, :write_primary, :options]

  def new(tenant, schema, source, metadata, write_primary, options) do
    %__MODULE__{
      tenant: tenant,
      schema: schema,
      source: source,
      metadata: metadata,
      write_primary: write_primary,
      options: options
    }
  end

  def insert_one(
        acc,
        tx,
        {{pk_field, pk}, future, data_object},
        read_before_write
      ) do
    %__MODULE__{
      tenant: tenant,
      source: source
    } = acc

    kv_codec = Pack.primary_codec(tenant, source, pk)
    read_before_write = if kv_codec.vs?, do: false, else: read_before_write
    data_object = [{pk_field, pk} | Keyword.delete(data_object, pk_field)]
    kv = %DecodedKV{codec: kv_codec, data_object: data_object}

    if read_before_write do
      future = Tx.async_get(tenant, tx, kv_codec, future)
      Future.apply(future, &do_set(acc, tx, kv, &1))
    else
      # We assume that the data doesn't exist. This speeds up data loading
      # but can result in inconsistent indexes if objects do exist in
      # the database that are being blindly overwritten.
      Future.set_result(future, do_set(acc, tx, kv, nil))
    end
  end

  def do_set(acc, tx, new_kv, nil) do
    %__MODULE__{
      tenant: tenant,
      schema: schema,
      metadata: metadata,
      write_primary: write_primary,
      options: options
    } = acc

    %DecodedKV{codec: kv_codec, data_object: data_object} = new_kv

    {_, kvs} = PrimaryKVCodec.encode(kv_codec, Pack.to_fdb_value(data_object), options)

    if write_primary do
      PrimaryKVCodec.set_new_kvs(tx, kv_codec, kvs)
    end

    kv_codec = PrimaryKVCodec.with_packed_key(kv_codec)

    Indexer.set(tenant, tx, metadata, schema, {kv_codec, data_object})
    :ok
  end

  def do_set(acc, tx, new_kv, existing_kv) do
    %__MODULE__{
      tenant: tenant,
      schema: schema,
      metadata: metadata,
      write_primary: write_primary,
      options: options
    } = acc

    %DecodedKV{data_object: data_object = [{pk_field, pk} | _]} = new_kv

    case options[:on_conflict] do
      :nothing ->
        nil

      :replace_all ->
        Tx.update_data_object(
          tenant,
          tx,
          schema,
          pk_field,
          {existing_kv, [set: data_object]},
          metadata,
          write_primary,
          options
        )

        :ok

      {:replace_all_except, fields} ->
        Tx.update_data_object(
          tenant,
          tx,
          schema,
          pk_field,
          {existing_kv, [set: Keyword.drop(data_object, fields)]},
          metadata,
          write_primary,
          options
        )

        :ok

      {:replace, fields} ->
        Tx.update_data_object(
          tenant,
          tx,
          schema,
          pk_field,
          {existing_kv, [set: Keyword.take(data_object, fields)]},
          metadata,
          write_primary,
          options
        )

        :ok

      val when is_nil(val) or val == :raise ->
        raise Unsupported, "Key exists: #{inspect(schema)} #{inspect(pk)}"

      unsupported_on_conflict ->
        raise Unsupported, """
        The :on_conflict option provided is not supported by the FoundationDB Adapter.

        You provided #{inspect(unsupported_on_conflict)}.

        Instead, use one of :raise, :nothing, :replace_all, {:replace_all_except, fields}, or {:replace, fields}
        """
    end
  end
end
