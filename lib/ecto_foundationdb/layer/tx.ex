defmodule EctoFoundationDB.Layer.Tx do
  @moduledoc false
  alias EctoFoundationDB.Exception.IncorrectTenancy
  alias EctoFoundationDB.Exception.Unsupported
  alias EctoFoundationDB.Indexer
  alias EctoFoundationDB.Layer.Fields
  alias EctoFoundationDB.Layer.Pack
  alias EctoFoundationDB.Layer.TxInsert
  alias EctoFoundationDB.Schema
  alias EctoFoundationDB.Tenant

  @tenant :__ectofdbtxcontext__
  @tx :__ectofdbtx__

  def in_tenant_tx?() do
    tenant = Process.get(@tenant)
    flag = in_tx?() and tenant.__struct__ == Tenant
    {flag, tenant}
  end

  def in_tx?(), do: not is_nil(Process.get(@tx))

  def safe?(nil) do
    case in_tenant_tx?() do
      {true, tenant} ->
        {true, tenant}

      {false, _} ->
        {false, :missing_tenant}
    end
  end

  def safe?(tenant) do
    if tenant.__struct__ == Tenant do
      {true, tenant}
    else
      {false, :missing_tenant}
    end
  end

  def transactional_external(tenant, fun) do
    nil = Process.get(@tenant)
    nil = Process.get(@tx)

    :erlfdb.transactional(
      Tenant.txobj(tenant),
      fn tx ->
        Process.put(@tenant, tenant)
        Process.put(@tx, tx)

        try do
          cond do
            is_function(fun, 0) -> fun.()
            is_function(fun, 1) -> fun.(tx)
          end
        after
          Process.delete(@tx)
          Process.delete(@tenant)
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
    case Process.get(@tenant, nil) do
      nil ->
        try do
          Process.put(@tenant, context)

          :erlfdb.transactional(Tenant.txobj(context), fn tx ->
            Process.put(@tx, tx)
            fun.(tx)
          end)
        after
          Process.delete(@tx)
          Process.delete(@tenant)
        end

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

  def insert_all(tenant, tx, {schema, source, context}, entries, {idxs, partial_idxs}, options) do
    entries =
      entries
      |> Enum.map(fn {{pk_field, pk}, data_object} ->
        key = Pack.primary_pack(tenant, source, pk)

        data_object = Fields.to_front(data_object, pk_field)
        {key, data_object}
      end)

    write_primary = Schema.get_option(context, :write_primary)

    acc = TxInsert.new(schema, idxs, partial_idxs, write_primary, options)

    case options[:conflict_target] do
      [] ->
        # We pretend that the data doesn't exist. This speeds up data loading
        # but can result in inconsistent indexes if objects do exist in
        # the database that are being blindly overwritten.
        acc = Enum.reduce(entries, acc, &TxInsert.set_stage(tenant, tx, &1, :not_found, &2))
        acc.count

      nil ->
        acc = pipeline(tenant, tx, entries, &TxInsert.get_stage/2, acc, &TxInsert.set_stage/5)
        acc.count

      unsupported_conflict_target ->
        raise Unsupported, """
        The :conflict_target option provided is not supported by the FoundationDB Adapter.

        You provided #{inspect(unsupported_conflict_target)}.

        Instead, we suggest you do not use this option at all.

        FoundationDB Adapter does support `conflict_target: []`, but this using this option
        can result in inconsistent indexes, and it is only recommended if you know ahead of
        time that your data does not already exist in the database.
        """
    end
  end

  def update_pks(
        tenant,
        tx,
        {schema, source, context},
        pk_field,
        pks,
        set_data,
        {idxs, partial_idxs}
      ) do
    keys = for pk <- pks, do: Pack.primary_pack(tenant, source, pk)

    write_primary = Schema.get_option(context, :write_primary)

    get_stage = &:erlfdb.get/2

    update_stage = fn
      _tenant, _tx, _key, :not_found, acc ->
        acc

      _tenant, tx, fdb_key, fdb_value, acc ->
        data_object = Pack.from_fdb_value(fdb_value)

        update_data_object(
          tenant,
          tx,
          schema,
          pk_field,
          {fdb_key, data_object},
          [set: set_data],
          {idxs, partial_idxs},
          write_primary
        )

        acc + 1
    end

    pipeline(tenant, tx, keys, get_stage, 0, update_stage)
  end

  def update_data_object(
        tenant,
        tx,
        schema,
        pk_field,
        {fdb_key, orig_data_object},
        updates,
        {idxs, partial_idxs},
        write_primary
      ) do
    orig_data_object = Fields.to_front(orig_data_object, pk_field)
    data_object = Keyword.merge(orig_data_object, updates[:set])

    if write_primary, do: :erlfdb.set(tx, fdb_key, Pack.to_fdb_value(data_object))

    Indexer.update(tenant, tx, idxs, partial_idxs, schema, {fdb_key, orig_data_object}, updates)
  end

  def delete_pks(tenant, tx, {schema, source, _context}, pks, {idxs, partial_idxs}) do
    keys = for pk <- pks, do: Pack.primary_pack(tenant, source, pk)

    get_stage = &:erlfdb.get/2

    clear_stage = fn
      _tenant, _tx, _key, :not_found, acc ->
        acc

      _tenant, tx, fdb_key, fdb_value, acc ->
        delete_data_object(
          tenant,
          tx,
          schema,
          {fdb_key, Pack.from_fdb_value(fdb_value)},
          {idxs, partial_idxs}
        )

        acc + 1
    end

    pipeline(tenant, tx, keys, get_stage, 0, clear_stage)
  end

  def delete_data_object(tenant, tx, schema, kv = {fdb_key, _}, {idxs, partial_idxs}) do
    :erlfdb.clear(tx, fdb_key)

    Indexer.clear(tenant, tx, idxs, partial_idxs, schema, kv)
  end

  def clear_all(tenant, tx, %{opts: _adapter_opts}, source) do
    # this key prefix will clear datakeys and indexkeys, but not user data or migration data
    {key_start, key_end} = Pack.adapter_source_range(tenant, source)

    # this would be a lot faster if we didn't have to count the keys
    num = count_range(tx, key_start, key_end)
    :erlfdb.clear_range(tx, key_start, key_end)
    num
  end

  def watch(tenant, tx, {_schema, source, context}, {_pk_field, pk}, _options) do
    if not Schema.get_option(context, :write_primary) do
      raise Unsupported, "Watches on schemas with `write_primary: false` are not supported."
    end

    fut = :erlfdb.watch(tx, Pack.primary_pack(tenant, source, pk))
    fut
  end

  defp count_range(tx, key_start, key_end) do
    :erlfdb.fold_range(tx, key_start, key_end, fn _kv, acc -> acc + 1 end, 0)
  end

  @doc false
  # A multi-stage pipeline to be executed within a transaction where
  # the first stage induces a list of futures, and the second stage
  # handles those futures as they arrive (using :erlfdb.wait_for_any/1).

  # tenant: Tenant.t()
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
  defp pipeline(tenant, tx, input_list, fun_stage_1, acc, fun_stage_2) do
    futures_map =
      input_list
      |> Enum.map(fn x ->
        fut = fun_stage_1.(tx, x)
        {fut, x}
      end)
      |> Enum.into(%{})

    result = fold_futures(tenant, tx, futures_map, acc, fun_stage_2)

    result
  end

  defp fold_futures(tenant, tx, futures_map, acc, fun) do
    fold_futures(tenant, tx, Map.keys(futures_map), futures_map, acc, fun)
  end

  defp fold_futures(_tenant, _tx, [], _futures_map, acc, _fun) do
    acc
  end

  defp fold_futures(tenant, tx, futures, futures_map, acc, fun) do
    fut = :erlfdb.wait_for_any(futures)

    case Map.get(futures_map, fut, nil) do
      nil ->
        :erlang.error(:badarg)

      map_entry ->
        futures = futures -- [fut]
        acc = fun.(tenant, tx, map_entry, :erlfdb.get(fut), acc)
        fold_futures(tenant, tx, futures, futures_map, acc, fun)
    end
  end
end
