defmodule EctoFoundationDB.Layer.TxInsert do
  @moduledoc false
  alias EctoFoundationDB.Exception.Unsupported
  alias EctoFoundationDB.Indexer
  alias EctoFoundationDB.Layer.Pack
  alias EctoFoundationDB.Layer.Tx

  defstruct [:schema, :idxs, :partial_idxs, :write_primary, :options, :count]

  def new(schema, idxs, partial_idxs, write_primary, options) do
    %__MODULE__{
      schema: schema,
      idxs: idxs,
      partial_idxs: partial_idxs,
      write_primary: write_primary,
      options: options,
      count: 0
    }
  end

  def get_stage(tx, {key, _data_object}) do
    :erlfdb.get(tx, key)
  end

  def set_stage(tx, {fdb_key, data_object}, :not_found, acc) do
    %__MODULE__{
      schema: schema,
      idxs: idxs,
      partial_idxs: partial_idxs,
      write_primary: write_primary,
      options: _options,
      count: count
    } = acc

    fdb_value = Pack.to_fdb_value(data_object)

    if write_primary, do: :erlfdb.set(tx, fdb_key, fdb_value)
    Indexer.set(tx, idxs, partial_idxs, schema, {fdb_key, data_object})
    %__MODULE__{acc | count: count + 1}
  end

  def set_stage(tx, {fdb_key, data_object = [{pk_field, _} | _]}, result, acc) do
    %__MODULE__{
      schema: schema,
      idxs: idxs,
      partial_idxs: partial_idxs,
      write_primary: write_primary,
      options: options,
      count: count
    } = acc

    case options[:on_conflict] do
      :nothing ->
        acc

      :replace_all ->
        existing_object = Pack.from_fdb_value(result)

        Tx.update_data_object(
          tx,
          schema,
          pk_field,
          {fdb_key, existing_object},
          [set: data_object],
          idxs,
          partial_idxs,
          write_primary
        )

        %__MODULE__{acc | count: count + 1}

      {:replace_all_except, fields} ->
        existing_object = Pack.from_fdb_value(result)

        Tx.update_data_object(
          tx,
          schema,
          pk_field,
          {fdb_key, existing_object},
          [set: Keyword.drop(data_object, fields)],
          idxs,
          partial_idxs,
          write_primary
        )

        %__MODULE__{acc | count: count + 1}

      {:replace, fields} ->
        existing_object = Pack.from_fdb_value(result)

        Tx.update_data_object(
          tx,
          schema,
          pk_field,
          {fdb_key, existing_object},
          [set: Keyword.take(data_object, fields)],
          idxs,
          partial_idxs,
          write_primary
        )

        %__MODULE__{acc | count: count + 1}

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
