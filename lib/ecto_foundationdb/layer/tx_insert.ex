defmodule EctoFoundationDB.Layer.TxInsert do
  @moduledoc false
  alias EctoFoundationDB.Exception.Unsupported
  alias EctoFoundationDB.Indexer
  alias EctoFoundationDB.Layer.Pack
  alias EctoFoundationDB.Layer.Splayer
  alias EctoFoundationDB.Layer.Tx

  defstruct [:schema, :idxs, :partial_idxs, :write_primary, :options]

  def new(schema, idxs, partial_idxs, write_primary, options) do
    %__MODULE__{
      schema: schema,
      idxs: idxs,
      partial_idxs: partial_idxs,
      write_primary: write_primary,
      options: options
    }
  end

  def do_set(acc, tenant, tx, {splayer, data_object}, :not_found) do
    %__MODULE__{
      schema: schema,
      idxs: idxs,
      partial_idxs: partial_idxs,
      write_primary: write_primary,
      options: _options
    } = acc

    fdb_key = Splayer.pack(splayer, nil)

    fdb_value = Pack.to_fdb_value(data_object)

    if write_primary, do: :erlfdb.set(tx, fdb_key, fdb_value)
    Indexer.set(tenant, tx, idxs, partial_idxs, schema, {fdb_key, data_object})
    :ok
  end

  def do_set(acc, tenant, tx, {splayer, data_object = [{pk_field, _} | _]}, result) do
    %__MODULE__{
      schema: schema,
      idxs: idxs,
      partial_idxs: partial_idxs,
      write_primary: write_primary,
      options: options
    } = acc

    fdb_key = Splayer.pack(splayer, nil)

    case options[:on_conflict] do
      :nothing ->
        nil

      :replace_all ->
        existing_object = Pack.from_fdb_value(result)

        Tx.update_data_object(
          tenant,
          tx,
          schema,
          pk_field,
          {fdb_key, existing_object},
          [set: data_object],
          {idxs, partial_idxs},
          write_primary
        )

        :ok

      {:replace_all_except, fields} ->
        existing_object = Pack.from_fdb_value(result)

        Tx.update_data_object(
          tenant,
          tx,
          schema,
          pk_field,
          {fdb_key, existing_object},
          [set: Keyword.drop(data_object, fields)],
          {idxs, partial_idxs},
          write_primary
        )

        :ok

      {:replace, fields} ->
        existing_object = Pack.from_fdb_value(result)

        Tx.update_data_object(
          tenant,
          tx,
          schema,
          pk_field,
          {fdb_key, existing_object},
          [set: Keyword.take(data_object, fields)],
          {idxs, partial_idxs},
          write_primary
        )

        :ok

      val when is_nil(val) or val == :raise ->
        raise Unsupported, "Key exists: #{inspect(fdb_key, binaries: :as_strings)}"

      unsupported_on_conflict ->
        raise Unsupported, """
        The :on_conflict option provided is not supported by the FoundationDB Adapter.

        You provided #{inspect(unsupported_on_conflict)}.

        Instead, use one of :raise, :nothing, :replace_all, {:replace_all_except, fields}, or {:replace, fields}
        """
    end
  end
end
