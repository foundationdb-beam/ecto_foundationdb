defmodule Ecto.Adapters.FoundationDB.Record.Tx do
  alias Ecto.Adapters.FoundationDB.Record.Fields
  alias Ecto.Adapters.FoundationDB.Record.Pack

  @db_or_tenant :__ectofdbtxcontext__
  @tx :__ectofdbtx__

  def in_tenant_tx?(), do: in_tx?() and elem(Process.get(@db_or_tenant), 0) == :erlfdb_tenant
  def in_tx?(), do: not is_nil(Process.get(@tx))

  # Structs can be passed from one tenant to another transaction, so if the tenant is missing, raise an error to prevent cross contamination
  def is_safe?(:struct, nil, true), do: if(in_tenant_tx?(), do: true, else: {false, :missing_tenant})
  def is_safe?(:struct, _tenant, true), do: true
  def is_safe?(:struct, nil, false), do: {false, :tenant_only}
  def is_safe?(:struct, _tenant, false), do: {false, :unused_tenant}

  # If a Query is missing a tenant and we're in a tx, allow. If queries are passed between tenants, there is no risk of cross contamination, like there is with structs
  def is_safe?(:query, nil, true), do: if(in_tenant_tx?(), do: true, else: {false, :missing_tenant})
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
         raise """
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
        raise """
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

    db_or_tenant
    |> transactional(fn tx ->
      Enum.map(entries, fn {key, value} -> :erlfdb.set(tx, key, value) end)
      :ok
    end)
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
          :erlfdb.get(tx, key)
          |> :erlfdb.wait()
        end)
        |> Pack.from_fdb_value()
        |> Fields.arrange(field_names)
        |> enlist()

      _ ->
        raise "FoundationDB Adapter does not support a where clause constraining on a field other than the primary key"
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

    Enum.map(kvs, fn {_k, value} ->
      value
      |> Pack.from_fdb_value()
      |> Fields.arrange(field_names)
    end)
  end

  def all(_db_or_tenant, _, query, _) do
    IO.inspect(Map.drop(query, [:__struct__]))
  end

  defp enlist(x), do: [x]
end
