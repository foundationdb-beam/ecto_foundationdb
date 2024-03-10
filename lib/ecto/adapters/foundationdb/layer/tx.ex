defmodule Ecto.Adapters.FoundationDB.Layer.Tx do
  @moduledoc """
  This internal module handles the execution of `:erlfdb` operations within transactions.
  """
  alias Ecto.Adapters.FoundationDB.Exception.IncorrectTenancy
  alias Ecto.Adapters.FoundationDB.Exception.Unsupported
  alias Ecto.Adapters.FoundationDB.Layer.Fields
  alias Ecto.Adapters.FoundationDB.Layer.Pack
  alias Ecto.Adapters.FoundationDB.Schema

  @db_or_tenant :__ectofdbtxcontext__
  @tx :__ectofdbtx__

  def in_tenant_tx?(), do: in_tx?() and elem(Process.get(@db_or_tenant), 0) == :erlfdb_tenant
  def in_tx?(), do: not is_nil(Process.get(@tx))

  def safe?(tenant, nil), do: safe?(tenant, false)

  def safe?(nil, true),
    do: if(in_tenant_tx?(), do: true, else: {false, :missing_tenant})

  def safe?(_tenant, true), do: true
  def safe?(nil, false), do: {false, :tenant_only}
  def safe?(_tenant, false), do: {false, :unused_tenant}

  def commit_proc(db_or_tenant, fun) when is_function(fun, 0) do
    nil = Process.get(@db_or_tenant)
    nil = Process.get(@tx)

    :erlfdb.transactional(
      db_or_tenant,
      fn tx ->
        Process.put(@db_or_tenant, db_or_tenant)
        Process.put(@tx, tx)

        try do
          fun.()
        after
          Process.delete(@tx)
          Process.delete(@db_or_tenant)
        end
      end
    )
  end

  def transactional(nil, fun) do
    case Process.get(@tx, nil) do
      nil ->
        raise IncorrectTenancy, """
        FoundationDB Adapter has no transactional context to execute on.
        """

      tx ->
        fun.(tx)
    end
  end

  def transactional(context, fun) do
    case Process.get(@db_or_tenant, nil) do
      nil ->
        :erlfdb.transactional(context, fun)

      ^context ->
        tx = Process.get(@tx, nil)
        fun.(tx)

      orig ->
        raise IncorrectTenancy, """
        FoundationDB Adapter encountered a transaction where the original transaction context \
        #{inspect(orig)} did not match the prefix on a struct or query within the transaction: \
        #{inspect(context)}.

        This can be encountered when a struct read from one tenant is provided to a transaction from \
        another. In these cases, the prefix must explicitly be removed from the struct metadata.
        """
    end
  end

  def insert_all(tx, %{opts: adapter_opts}, source, context, entries, idxs) do
    entries =
      entries
      |> Enum.map(fn {{pk_field, pk}, data_object} ->
        key =
          Pack.to_fdb_datakey(adapter_opts, source, pk)

        data_object = Fields.to_front(data_object, pk_field)
        {key, data_object}
      end)

    write_primary = Schema.get_option(context, :write_primary)

    get_stage = fn tx, {key, _data_object} -> :erlfdb.get(tx, key) end

    set_stage = fn
      tx, {fdb_key, data_object}, :not_found, count ->
        fdb_value = Pack.to_fdb_value(data_object)

        if write_primary, do: do_set(tx, fdb_key, fdb_value)
        set_indexes_for_one(tx, idxs, adapter_opts, {fdb_key, data_object}, source)
        count + 1

      _tx, {key, _}, _result, _acc ->
        raise Unsupported, "Key exists: #{key}"
    end

    pipeline(tx, entries, get_stage, 0, set_stage)
  end

  def update_pks(
        tx,
        adapter_meta = %{opts: adapter_opts},
        source,
        context,
        pk_field,
        pks,
        set_data,
        idxs
      ) do
    keys = for pk <- pks, do: Pack.to_fdb_datakey(adapter_opts, source, pk)

    write_primary = Schema.get_option(context, :write_primary)

    get_stage = &:erlfdb.get/2

    update_stage = fn
      _tx, _key, :not_found, acc ->
        acc

      tx, fdb_key, fdb_value, acc ->
        data_object = Pack.from_fdb_value(fdb_value)

        update_data_object(
          tx,
          adapter_meta,
          source,
          pk_field,
          {fdb_key, data_object},
          [set: set_data],
          idxs,
          write_primary
        )

        acc + 1
    end

    pipeline(tx, keys, get_stage, 0, update_stage)
  end

  def update_data_object(
        tx,
        %{opts: adapter_opts},
        source,
        pk_field,
        {fdb_key, data_object},
        updates,
        idxs,
        write_primary
      ) do
    set_data = Keyword.get(updates, :set, [])

    data_object =
      data_object
      |> Keyword.merge(set_data, fn _k, _v1, v2 -> v2 end)
      |> Fields.to_front(pk_field)

    if write_primary, do: do_set(tx, fdb_key, Pack.to_fdb_value(data_object))
    clear_indexes_for_one(tx, idxs, adapter_opts, {fdb_key, data_object}, source)
    set_indexes_for_one(tx, idxs, adapter_opts, {fdb_key, data_object}, source)
  end

  def delete_pks(tx, adapter_meta = %{opts: adapter_opts}, source, pks, idxs) do
    keys = for pk <- pks, do: Pack.to_fdb_datakey(adapter_opts, source, pk)

    get_stage = &:erlfdb.get/2

    clear_stage = fn
      _tx, _key, :not_found, acc ->
        acc

      tx, fdb_key, fdb_value, acc ->
        delete_data_object(
          tx,
          adapter_meta,
          source,
          {fdb_key, Pack.from_fdb_value(fdb_value)},
          idxs
        )

        acc + 1
    end

    pipeline(tx, keys, get_stage, 0, clear_stage)
  end

  def delete_data_object(tx, %{opts: adapter_opts}, source, kv = {fdb_key, _}, idxs) do
    :erlfdb.clear(tx, fdb_key)

    clear_indexes_for_one(
      tx,
      idxs,
      adapter_opts,
      kv,
      source
    )
  end

  def clear_all(tx, %{opts: adapter_opts}, source) do
    # this key prefix will clear datakeys and indexkeys
    key_startswith = Pack.to_raw_fdb_key(adapter_opts, [source, ""])

    num = count_range_startswith(tx, key_startswith)
    :erlfdb.clear_range_startswith(tx, key_startswith)
    num
  end

  def create_index(
        tx,
        %{opts: adapter_opts},
        source,
        index_name,
        index_fields,
        options,
        {inventory_key, inventory_value}
      ) do
    key_startswith = Pack.to_fdb_datakey_startswith(adapter_opts, source)
    key_start = key_startswith
    key_end = :erlfdb_key.strinc(key_startswith)

    # Prevent updates on the keys so that we write the correct index values
    :erlfdb.add_write_conflict_range(tx, key_start, key_end)

    # Write a key that indicates the index exists. All other operations will
    # use this info to maintain the index
    do_set(tx, inventory_key, inventory_value)

    # Write the actual index for any existing data in this tenant
    tx
    |> :erlfdb.get_range(key_start, key_end)
    |> :erlfdb.wait()
    |> Enum.map(fn {fdb_key, fdb_value} ->
      {index_key, index_object} =
        get_index_entry(
          adapter_opts,
          {fdb_key, Pack.from_fdb_value(fdb_value)},
          index_fields,
          options,
          index_name,
          source
        )

      do_set(tx, index_key, index_object)
    end)
  end

  defp count_range_startswith(tx, startswith) do
    start_key = startswith
    end_key = :erlfdb_key.strinc(startswith)

    :erlfdb.fold_range(
      tx,
      start_key,
      end_key,
      fn _kv, acc ->
        acc + 1
      end,
      0
    )
  end

  @doc false
  # A multi-stage pipeline to be executed within a transaction where
  # the first stage induces a list of futures, and the second stage
  # handles those futures as they arrive (using :erlfdb.wait_for_any/1).

  # tx: The erlfdb tx
  # input_list: a list of items to be handled by your stages
  # fun_stage_1: a 2-arity function accepting as args (tx, x) where
  # tx is the same erlfdb tx and x is an entry from input_list. This function
  # must return an erlfdb future.
  # acc: Starting accumulator for fun_stage_2
  # fun_stage_2: a 4-arity function acceptiong as aargs (tx, x, result, acc)
  # where tx is the same erlfdb tx, x is the corresponding entry in your
  # input_list, result is the result of the future from stage_1, and acc is
  # the accumulator. This function returns the updated acc.
  defp pipeline(tx, input_list, fun_stage_1, acc, fun_stage_2) do
    futures_map =
      input_list
      |> Enum.map(fn x ->
        fut = fun_stage_1.(tx, x)
        {fut, x}
      end)
      |> Enum.into(%{})

    result = fold_futures(tx, futures_map, acc, fun_stage_2)

    result
  end

  defp fold_futures(tx, futures_map, acc, fun) do
    fold_futures(tx, Map.keys(futures_map), futures_map, acc, fun)
  end

  defp fold_futures(_tx, [], _futures_map, acc, _fun) do
    acc
  end

  defp fold_futures(tx, futures, futures_map, acc, fun) do
    fut = :erlfdb.wait_for_any(futures)

    case Map.get(futures_map, fut, nil) do
      nil ->
        :erlang.error(:badarg)

      map_entry ->
        futures = futures -- [fut]
        acc = fun.(tx, map_entry, :erlfdb.get(fut), acc)
        fold_futures(tx, futures, futures_map, acc, fun)
    end
  end

  # Note: pk is always first. See insert and update paths
  defp get_index_entry(
         adapter_opts,
         {fdb_key, data_object = [{pk_field, pk_value} | _]},
         index_fields,
         index_options,
         index_name,
         source
       ) do
    index_fields = index_fields -- [pk_field]

    index_entries =
      for idx_field <- index_fields, do: {idx_field, Keyword.get(data_object, idx_field)}

    {_, path_vals} = Enum.unzip(index_entries)

    index_key =
      Pack.to_fdb_indexkey(
        adapter_opts,
        index_options,
        source,
        "#{index_name}",
        path_vals,
        pk_value
      )

    index_object =
      Pack.new_index_object(fdb_key, data_object)
      |> Pack.to_fdb_value()

    {index_key, index_object}
  end

  defp clear_indexes_for_one(tx, idxs, adapter_opts, kv, source) do
    for idx <- idxs do
      index_name = idx[:id]
      index_fields = idx[:fields]
      index_options = idx[:options]

      {index_key, _index_object} =
        get_index_entry(
          adapter_opts,
          kv,
          index_fields,
          index_options,
          index_name,
          source
        )

      :erlfdb.clear(tx, index_key)
    end
  end

  defp set_indexes_for_one(tx, idxs, adapter_opts, kv, source) do
    for idx <- idxs do
      index_name = idx[:id]
      index_fields = idx[:fields]
      index_options = idx[:options]

      {index_key, index_object} =
        get_index_entry(
          adapter_opts,
          kv,
          index_fields,
          index_options,
          index_name,
          source
        )

      do_set(tx, index_key, index_object)
    end
  end

  defp do_set(tx, fdb_key, fdb_value) do
    :erlfdb.set(tx, fdb_key, fdb_value)
  end
end
