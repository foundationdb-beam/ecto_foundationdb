defmodule EctoFoundationDB.Layer.TxInsert do
  @moduledoc false
  alias EctoFoundationDB.Exception.Unsupported
  alias EctoFoundationDB.Indexer
  alias EctoFoundationDB.Layer.Pack
  alias EctoFoundationDB.Layer.KVZipper
  alias EctoFoundationDB.Layer.Tx

  defstruct [:tenant, :schema, :idxs, :partial_idxs, :write_primary, :options]

  def new(tenant, schema, idxs, partial_idxs, write_primary, options) do
    %__MODULE__{
      tenant: tenant,
      schema: schema,
      idxs: idxs,
      partial_idxs: partial_idxs,
      write_primary: write_primary,
      options: options
    }
  end

  def do_set(acc, tx, {zipper, data_object}, :not_found) do
    %__MODULE__{
      tenant: tenant,
      schema: schema,
      idxs: idxs,
      partial_idxs: partial_idxs,
      write_primary: write_primary,
      options: options
    } = acc

    {_, kvs} = KVZipper.unzip(zipper, Pack.to_fdb_value(data_object), options)

    if write_primary do
      for {k, v} <- kvs, do: :erlfdb.set(tx, k, v)
    end

    # The indexer is not informed of the object splitting
    fdb_key = KVZipper.pack_key(zipper, nil)

    Indexer.set(tenant, tx, idxs, partial_idxs, schema, {fdb_key, data_object})
    :ok
  end

  def do_set(acc, tx, {zipper, data_object = [{pk_field, pk} | _]}, existing_object) do
    %__MODULE__{
      tenant: tenant,
      schema: schema,
      idxs: idxs,
      partial_idxs: partial_idxs,
      write_primary: write_primary,
      options: options
    } = acc

    case options[:on_conflict] do
      :nothing ->
        nil

      :replace_all ->
        Tx.update_data_object(
          tenant,
          tx,
          schema,
          pk_field,
          {zipper, existing_object},
          [set: data_object],
          {idxs, partial_idxs},
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
          {zipper, existing_object},
          [set: Keyword.drop(data_object, fields)],
          {idxs, partial_idxs},
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
          {zipper, existing_object},
          [set: Keyword.take(data_object, fields)],
          {idxs, partial_idxs},
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
