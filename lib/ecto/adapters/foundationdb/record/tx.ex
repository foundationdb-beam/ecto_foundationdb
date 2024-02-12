defmodule Ecto.Adapters.FoundationDB.Record.Tx do
  alias Ecto.Adapters.FoundationDB.IndexInventory
  alias Ecto.Adapters.FoundationDB.Record.Fields
  alias Ecto.Adapters.FoundationDB.Record.Pack
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

  def insert_all(db_or_tenant, adapter_meta = %{opts: adapter_opts}, source, entries) do
    idxs = IndexInventory.get_for_source(adapter_meta, db_or_tenant, source)

    entries =
      entries
      |> Enum.map(fn {{pk_field, pk}, fields} ->
        key = Pack.to_fdb_datakey(adapter_opts, source, pk)
        fields = Fields.to_front(fields, pk_field)
        {key, fields}
      end)

    get_stage = fn tx, {key, _value} -> :erlfdb.get(tx, key) end

    set_stage = fn
      tx, {fdb_key, fields}, :not_found, count ->
        fdb_value = Pack.to_fdb_value(fields)
        :erlfdb.set(tx, fdb_key, fdb_value)
        set_indexes_for_one(tx, idxs, adapter_opts, fdb_key, fields, source)
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
        fields
      ) do
    idxs = IndexInventory.get_for_source(adapter_meta, db_or_tenant, source)

    keys = for pk <- pks, do: Pack.to_fdb_datakey(adapter_opts, source, pk)

    get_stage = fn tx, key -> :erlfdb.get(tx, key) end

    update_stage = fn
      _tx, _key, :not_found, acc ->
        acc

      tx, fdb_key, fdb_value, acc ->
        value =
          fdb_value
          |> Pack.from_fdb_value()
          |> Keyword.merge(fields, fn _k, _v1, v2 -> v2 end)
          |> Fields.to_front(pk_field)

        :erlfdb.set(tx, fdb_key, Pack.to_fdb_value(value))
        clear_indexes_for_one(tx, idxs, adapter_opts, fdb_key, value, source)
        set_indexes_for_one(tx, idxs, adapter_opts, fdb_key, value, source)

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

  # Single where clause expression matching on the primary key or index
  def all(
        db_or_tenant,
        adapter_meta = %{opts: adapter_opts},
        %Ecto.Query{
          select: %Ecto.Query.SelectExpr{
            fields: select_fields
          },
          from: %Ecto.Query.FromExpr{source: {source, schema}},
          wheres: [
            %Ecto.Query.BooleanExpr{
              expr: {:==, [], [{{:., [], [{:&, [], [0]}, where_field]}, [], []}, {:^, [], [0]}]}
            }
          ]
        },
        [where_value]
      ) do
    idxs = IndexInventory.get_for_source(adapter_meta, db_or_tenant, source)
    field_names = Fields.parse_select_fields(select_fields)

    case Fields.get_pk_field!(schema) do
      ^where_field ->
        key = Pack.to_fdb_datakey(adapter_opts, source, where_value)

        db_or_tenant
        |> transactional(fn tx ->
          value = :erlfdb.wait(:erlfdb.get(tx, key))
          [{key, value}]
        end)
        |> handle_fdb_kvs(field_names)

      _pk_field ->
        case IndexInventory.select_index(idxs, [where_field]) do
          {:ok, idx} ->
            index_name = idx[:id]

            indexkey_startswith =
              Pack.to_fdb_indexkey(adapter_opts, source, index_name, [where_value])
              |> Pack.add_delimiter(adapter_opts)

            db_or_tenant
            |> transactional(fn tx ->
              tx
              |> :erlfdb.get_range_startswith(indexkey_startswith)
              |> :erlfdb.wait()
              |> Enum.map(fn {_index_fdb_key, index_fdb_value} ->
                index_object = Pack.from_fdb_value(index_fdb_value)
                index_object[:value]
              end)
            end)
            |> Enum.map(fn value -> Fields.arrange(value, field_names) end)

          {:error, _} ->
            raise Unsupported,
                  """
                  FoundationDB Adapter does not support a where clause constraining on a field other than the primary key or an index.
                  """
        end
    end
  end

  # No where clause, all records
  def all(
        db_or_tenant,
        %{opts: adapter_opts},
        %Ecto.Query{
          select: %Ecto.Query.SelectExpr{fields: select_fields},
          from: %Ecto.Query.FromExpr{source: {source, _schema}},
          wheres: []
        },
        []
      ) do
    field_names = Fields.parse_select_fields(select_fields)
    key_startswith = Pack.to_fdb_datakey_startswith(adapter_opts, source)

    kvs =
      transactional(db_or_tenant, fn tx ->
        tx
        |> :erlfdb.get_range_startswith(key_startswith)
        |> :erlfdb.wait()
      end)

    handle_fdb_kvs(kvs, field_names)
  end

  def all(_db_or_tenant, _, query, _) do
    raise Unsupported, """
    FoundationDB Adapater has not implemented support for your query

    #{inspect(Map.drop(query, [:__struct__]))}
    """
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

  defp handle_fdb_kvs(kvs, field_names) do
    kvs
    |> Enum.filter(fn
      {_k, :not_found} -> false
      {_k, _v} -> true
    end)
    |> Enum.map(fn
      {_k, value} ->
        value
        |> Pack.from_fdb_value()
        |> Fields.arrange(field_names)
    end)
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
         value = [{pk_field, pk_value} | _],
         index_fields,
         index_name,
         source
       ) do
    index_fields = index_fields -- [pk_field]
    index_entries = for idx_field <- index_fields, do: {idx_field, Keyword.get(value, idx_field)}
    path_entries = index_entries ++ [{pk_field, pk_value}]
    {_, path_vals} = Enum.unzip(path_entries)

    index_key = Pack.to_fdb_indexkey(adapter_opts, source, "#{index_name}", path_vals)

    index_object =
      Pack.new_index_object(source, fdb_key, pk_field, pk_value, index_entries, value)
      |> Pack.to_fdb_value()

    {index_key, index_object}
  end

  defp clear_indexes_for_one(tx, idxs, adapter_opts, fdb_key, value, source) do
    for idx <- idxs do
      index_name = idx[:id]
      index_fields = idx[:fields]

      {index_key, _index_object} =
        get_index_entry(
          adapter_opts,
          fdb_key,
          value,
          index_fields,
          index_name,
          source
        )

      :erlfdb.clear(tx, index_key)
    end
  end

  defp set_indexes_for_one(tx, idxs, adapter_opts, fdb_key, value, source) do
    for idx <- idxs do
      index_name = idx[:id]
      index_fields = idx[:fields]

      {index_key, index_object} =
        get_index_entry(
          adapter_opts,
          fdb_key,
          value,
          index_fields,
          index_name,
          source
        )

      :erlfdb.set(tx, index_key, index_object)
    end
  end
end
