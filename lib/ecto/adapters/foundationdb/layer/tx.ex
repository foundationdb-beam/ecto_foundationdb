defmodule Ecto.Adapters.FoundationDB.Layer.Tx do
  alias Ecto.Adapters.FoundationDB.QueryPlan
  alias Ecto.Adapters.FoundationDB.Layer.IndexInventory
  alias Ecto.Adapters.FoundationDB.Layer.Fields
  alias Ecto.Adapters.FoundationDB.Layer.Pack
  alias Ecto.Adapters.FoundationDB.Layer.Query
  alias Ecto.Adapters.FoundationDB.Schema
  alias Ecto.Adapters.FoundationDB.Exception.IncorrectTenancy
  alias Ecto.Adapters.FoundationDB.Exception.Unsupported

  @db_or_tenant :__ectofdbtxcontext__
  @tx :__ectofdbtx__

  def in_tenant_tx?(), do: in_tx?() and elem(Process.get(@db_or_tenant), 0) == :erlfdb_tenant
  def in_tx?(), do: not is_nil(Process.get(@tx))

  def is_safe?(tenant, nil), do: is_safe?(tenant, false)

  def is_safe?(nil, true),
    do: if(in_tenant_tx?(), do: true, else: {false, :missing_tenant})

  def is_safe?(tenant, true) when is_binary(tenant), do: {false, :tenant_id}
  def is_safe?(_tenant, true), do: true
  def is_safe?(nil, false), do: {false, :tenant_only}
  def is_safe?(_tenant, false), do: {false, :unused_tenant}

  def commit_proc(db_or_tenant, fun) do
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

  def insert_all(db_or_tenant, adapter_meta = %{opts: adapter_opts}, source, context, entries) do
    idxs = IndexInventory.get_for_source(adapter_meta, db_or_tenant, source)

    entries =
      entries
      |> Enum.map(fn {{pk_field, pk}, data_object} ->
        key = Pack.to_fdb_datakey(adapter_opts, source, pk)
        data_object = Fields.to_front(data_object, pk_field)
        {key, data_object}
      end)

    write_primary = Schema.get_option(context, :write_primary)

    get_stage = fn tx, {key, _data_object} -> :erlfdb.get(tx, key) end

    set_stage = fn
      tx, {fdb_key, data_object}, :not_found, count ->
        fdb_value = Pack.to_fdb_value(data_object)

        if write_primary do
          :erlfdb.set(tx, fdb_key, fdb_value)
        end

        set_indexes_for_one(tx, idxs, adapter_opts, fdb_key, data_object, source)
        count + 1

      _tx, {key, _}, _result, _acc ->
        raise Unsupported, "Key exists: #{key}"
    end

    transactional(db_or_tenant, fn tx -> pipeline(tx, entries, get_stage, 0, set_stage) end)
  end

  def update_pks(
        db_or_tenant,
        adapter_meta = %{opts: adapter_opts},
        source,
        pk_field,
        pks,
        update_data
      ) do
    idxs = IndexInventory.get_for_source(adapter_meta, db_or_tenant, source)

    keys = for pk <- pks, do: Pack.to_fdb_datakey(adapter_opts, source, pk)

    get_stage = fn tx, key -> :erlfdb.get(tx, key) end

    update_stage = fn
      _tx, _key, :not_found, acc ->
        acc

      tx, fdb_key, fdb_value, acc ->
        data_object =
          fdb_value
          |> Pack.from_fdb_value()
          |> Keyword.merge(update_data, fn _k, _v1, v2 -> v2 end)
          |> Fields.to_front(pk_field)

        :erlfdb.set(tx, fdb_key, Pack.to_fdb_value(data_object))
        clear_indexes_for_one(tx, idxs, adapter_opts, fdb_key, data_object, source)
        set_indexes_for_one(tx, idxs, adapter_opts, fdb_key, data_object, source)

        acc + 1
    end

    transactional(db_or_tenant, fn tx -> pipeline(tx, keys, get_stage, 0, update_stage) end)
  end

  def delete_pks(db_or_tenant, adapter_meta = %{opts: adapter_opts}, source, pks) do
    idxs = IndexInventory.get_for_source(adapter_meta, db_or_tenant, source)

    keys = for pk <- pks, do: Pack.to_fdb_datakey(adapter_opts, source, pk)

    get_stage = fn tx, key -> :erlfdb.get(tx, key) end

    clear_stage = fn
      _tx, _key, :not_found, acc ->
        acc

      tx, fdb_key, fdb_value, acc ->
        :erlfdb.clear(tx, fdb_key)

        clear_indexes_for_one(
          tx,
          idxs,
          adapter_opts,
          fdb_key,
          Pack.from_fdb_value(fdb_value),
          source
        )

        acc + 1
    end

    transactional(db_or_tenant, fn tx -> pipeline(tx, keys, get_stage, 0, clear_stage) end)
  end

  def all(
        db_or_tenant,
        adapter_meta,
        %Ecto.Query{
          select: %Ecto.Query.SelectExpr{
            fields: select_fields
          },
          from: %Ecto.Query.FromExpr{source: {source, schema}},
          wheres: wheres
        },
        params
      ) do
    # Steps:
    #   0. Validate wheres for supported query types
    #     i. Equal -> where_field == param[0]
    #     ii. Between -> where_field > param[0] and where_field < param[1]
    #     iii. None -> empty where clause
    #   1. pk or index?
    #   2. construct start key and end key from the first where expression
    #   3. Use :erlfdb.get, :erlfdb.get_range
    #   4. Post-get filtering (Remove :not_found, remove index conflicts, )
    #   5. Arrange fields based on the select input
    plan = QueryPlan.get(source, schema, wheres, params)
    kvs = Query.exec(db_or_tenant, adapter_meta, plan)

    field_names = Fields.parse_select_fields(select_fields)
    Enum.map(kvs, fn {_key, data_object} -> Fields.arrange(data_object, field_names) end)
  end

  # No where clause, all records
  def delete_all(
        db_or_tenant,
        %{opts: adapter_opts},
        %Ecto.Query{
          from: %Ecto.Query.FromExpr{source: {source, _schema}},
          wheres: []
        },
        []
      ) do
    # this key prefix will clear datakeys and indexkeys
    key_startswith = Pack.to_raw_fdb_key(adapter_opts, [source, ""])

    transactional(db_or_tenant, fn tx ->
      num = count_range_startswith(tx, key_startswith)
      :erlfdb.clear_range_startswith(tx, key_startswith)
      num
    end)
  end

  def create_index(
        db_or_tenant,
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

    transactional(db_or_tenant, fn tx ->
      # Prevent updates on the keys so that we write the correct index values
      :erlfdb.add_write_conflict_range(tx, key_start, key_end)

      # Write a key that indicates the index exists. All other operations will
      # use this info to maintain the index
      :erlfdb.set(tx, inventory_key, inventory_value)

      # Write the actual index for any existing data in this tenant
      tx
      |> :erlfdb.get_range(key_start, key_end)
      |> :erlfdb.wait()
      |> Enum.map(fn {fdb_key, fdb_value} ->
        {index_key, index_object} =
          get_index_entry(
            adapter_opts,
            fdb_key,
            Pack.from_fdb_value(fdb_value),
            index_fields,
            options,
            index_name,
            source
          )

        :erlfdb.set(tx, index_key, index_object)
      end)
    end)

    :ok
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
         fdb_key,
         data_object = [{pk_field, pk_value} | _],
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
      Pack.new_index_object(source, fdb_key, pk_field, pk_value, index_entries, data_object)
      |> Pack.to_fdb_value()

    {index_key, index_object}
  end

  defp clear_indexes_for_one(tx, idxs, adapter_opts, fdb_key, data_object, source) do
    for idx <- idxs do
      index_name = idx[:id]
      index_fields = idx[:fields]
      index_options = idx[:options]

      {index_key, _index_object} =
        get_index_entry(
          adapter_opts,
          fdb_key,
          data_object,
          index_fields,
          index_options,
          index_name,
          source
        )

      :erlfdb.clear(tx, index_key)
    end
  end

  defp set_indexes_for_one(tx, idxs, adapter_opts, fdb_key, data_object, source) do
    for idx <- idxs do
      index_name = idx[:id]
      index_fields = idx[:fields]
      index_options = idx[:options]

      {index_key, index_object} =
        get_index_entry(
          adapter_opts,
          fdb_key,
          data_object,
          index_fields,
          index_options,
          index_name,
          source
        )

      :erlfdb.set(tx, index_key, index_object)
    end
  end
end
