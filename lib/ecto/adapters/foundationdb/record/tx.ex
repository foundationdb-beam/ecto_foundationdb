defmodule Ecto.Adapters.FoundationDB.Record.Tx do
  alias Ecto.Adapters.FoundationDB.Record.Fields
  alias Ecto.Adapters.FoundationDB.Record.Pack
  alias Ecto.Adapters.FoundationDB.Exception.IncorrectTenancy
  alias Ecto.Adapters.FoundationDB.Exception.Unsupported

  @db_or_tenant :__ectofdbtxcontext__
  @tx :__ectofdbtx__

  def in_tenant_tx?(), do: in_tx?() and elem(Process.get(@db_or_tenant), 0) == :erlfdb_tenant
  def in_tx?(), do: not is_nil(Process.get(@tx))

  def is_safe?(type, tenant, nil), do: is_safe?(type, tenant, false)

  # Structs can be passed from one tenant to another transaction, so if the tenant is missing, raise an error to prevent cross contamination
  def is_safe?(:struct, nil, true),
    do: if(in_tenant_tx?(), do: true, else: {false, :missing_tenant})

  def is_safe?(:struct, _tenant, true), do: true
  def is_safe?(:struct, nil, false), do: {false, :tenant_only}
  def is_safe?(:struct, _tenant, false), do: {false, :unused_tenant}

  # If a Query is missing a tenant and we're in a tx, allow. If queries are passed between tenants, there is no risk of cross contamination, like there is with structs
  def is_safe?(:query, nil, true),
    do: if(in_tenant_tx?(), do: true, else: {false, :missing_tenant})

  def is_safe?(:query, _tenant, true), do: true
  def is_safe?(:query, nil, false), do: {false, :tenant_only}
  def is_safe?(:query, _tenant, false), do: {false, :unused_tenant}

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

  def insert_all(db_or_tenant, adapter_opts, source, entries) do
    entries =
      entries
      |> Enum.map(fn {pk, fields} ->
        key = Pack.to_fdb_key(adapter_opts, source, pk)
        value = Pack.to_fdb_value(fields)
        {key, value}
      end)

    get_stage = fn tx, {key, _value} -> :erlfdb.get(tx, key) end

    set_stage = fn
      tx, :not_found, {key, value}, acc ->
        :erlfdb.set(tx, key, value)
        acc + 1

      _tx, _result, {key, _}, _acc ->
        raise Unsupported, "Key exists: #{key}"
    end

    transactional(db_or_tenant, fn tx -> pipeline(tx, entries, get_stage, 0, set_stage) end)
  end

  def update_pks(db_or_tenant, adapter_opts, source, pks, fields) do
    keys = for pk <- pks, do: Pack.to_fdb_key(adapter_opts, source, pk)

    get_stage = fn tx, key -> :erlfdb.get(tx, key) end

    update_stage = fn
      _tx, :not_found, _key, acc ->
        acc

      tx, fdb_value, key, acc ->
        value =
          fdb_value
          |> Pack.from_fdb_value()
          |> Keyword.merge(fields, fn _k, _v1, v2 -> v2 end)

        :erlfdb.set(tx, key, Pack.to_fdb_value(value))
        acc + 1
    end

    transactional(db_or_tenant, fn tx -> pipeline(tx, keys, get_stage, 0, update_stage) end)
  end

  def delete_pks(db_or_tenant, adapter_opts, source, pks) do
    keys = for pk <- pks, do: Pack.to_fdb_key(adapter_opts, source, pk)

    get_stage = fn tx, key -> :erlfdb.get(tx, key) end

    clear_stage = fn
      _tx, :not_found, _key, acc ->
        acc

      tx, _fut_value, key, acc ->
        :erlfdb.clear(tx, key)
        acc + 1
    end

    transactional(db_or_tenant, fn tx -> pipeline(tx, keys, get_stage, 0, clear_stage) end)
  end

  # Single where clause expression matching on the primary key
  def all(
        db_or_tenant,
        adapter_opts,
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
        [pk]
      ) do
    case Fields.get_pk_field!(schema) do
      ^where_field ->
        field_names = Fields.parse_select_fields(select_fields)
        key = Pack.to_fdb_key(adapter_opts, source, pk)

        db_or_tenant
        |> transactional(fn tx ->
          value = :erlfdb.wait(:erlfdb.get(tx, key))
          [{key, value}]
        end)
        |> handle_fdb_kvs(field_names)

      _ ->
        raise Unsupported,
              "FoundationDB Adapter does not support a where clause constraining on a field other than the primary key"
    end
  end

  # No where clause, all records
  def all(
        db_or_tenant,
        adapter_opts,
        %Ecto.Query{
          select: %Ecto.Query.SelectExpr{fields: select_fields},
          from: %Ecto.Query.FromExpr{source: {source, _schema}},
          wheres: []
        },
        []
      ) do
    field_names = Fields.parse_select_fields(select_fields)
    key_startswith = Pack.to_fdb_key_startswith(adapter_opts, source)

    kvs =
      transactional(db_or_tenant, fn tx ->
        :erlfdb.get_range_startswith(tx, key_startswith)
        |> :erlfdb.wait()
      end)

    handle_fdb_kvs(kvs, field_names)
  end

  def all(_db_or_tenant, _, _query, _) do
    raise Unsupported, "FoundationDB Adapater has not implemented support for your query"
  end

  # No where clause, all records
  def delete_all(
        db_or_tenant,
        adapter_opts,
        %Ecto.Query{
          from: %Ecto.Query.FromExpr{source: {source, _schema}},
          wheres: []
        },
        []
      ) do
    key_startswith = Pack.to_fdb_key_startswith(adapter_opts, source)

    transactional(db_or_tenant, fn tx ->
      num = count_range_startswith(tx, key_startswith)
      :erlfdb.clear_range_startswith(tx, key_startswith)
      num
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
  # fun_stage_2: a 4-arity function acceptiong as aargs (tx, result, x, acc)
  # where tx is the same erlfdb tx, result, is the result of the future from
  # stage_1, x is the corresponding entry in your input_list, and acc is
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
        acc = fun.(tx, :erlfdb.get(fut), map_entry, acc)
        fold_futures(tx, futures, futures_map, acc, fun)
    end
  end
end
