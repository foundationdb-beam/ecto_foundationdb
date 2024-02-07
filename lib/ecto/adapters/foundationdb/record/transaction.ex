defmodule Ecto.Adapters.FoundationDB.Record.Transaction do
  alias Ecto.Adapters.FoundationDB.Record.Fields
  alias Ecto.Adapters.FoundationDB.Record.Pack

  def insert_all(db_or_tenant, adapter_opts, source, entries) do
    entries =
      entries
      |> Enum.map(fn {pk, fields} ->
        key = Pack.to_fdb_key(adapter_opts, source, pk)
        value = Pack.to_fdb_value(fields)
        {key, value}
      end)

    db_or_tenant
    |> :erlfdb.transactional(fn tx ->
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
        |> :erlfdb.transactional(fn tx ->
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
      :erlfdb.transactional(db_or_tenant, fn tx ->
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
