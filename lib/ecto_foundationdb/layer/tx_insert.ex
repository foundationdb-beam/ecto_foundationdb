defmodule EctoFoundationDB.Layer.TxInsert do
  @moduledoc false
  alias EctoFoundationDB.Exception.Unsupported
  alias EctoFoundationDB.Indexer
  alias EctoFoundationDB.Layer.DecodedKV
  alias EctoFoundationDB.Layer.Pack
  alias EctoFoundationDB.Layer.PrimaryKVCodec
  alias EctoFoundationDB.Layer.Tx

  defstruct [:tenant, :schema, :metadata, :write_primary, :options]

  def new(tenant, schema, metadata, write_primary, options) do
    %__MODULE__{
      tenant: tenant,
      schema: schema,
      metadata: metadata,
      write_primary: write_primary,
      options: options
    }
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
      for {k, v} <- kvs, do: :erlfdb.set(tx, k, v)
    end

    # The indexer is not informed of the object splitting
    fdb_key = PrimaryKVCodec.pack_key(kv_codec, nil)

    Indexer.set(tenant, tx, metadata, schema, {fdb_key, data_object})
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
