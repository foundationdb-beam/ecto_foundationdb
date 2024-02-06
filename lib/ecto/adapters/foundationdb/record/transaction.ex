defmodule Ecto.Adapters.FoundationDB.Record.Transaction do
  alias Ecto.Adapters.FoundationDB.Record.Fields

  def insert(adapter_opts, source, pk_field, pk, fields) do
    fields = Fields.pk_front(fields, pk_field)
    key = key(adapter_opts, source, pk)
    value = encode_value(fields)
    fn tx -> :erlfdb.set(tx, key, value) end
  end

  def all(
        adapter_opts,
        %Ecto.Query{
          from: %Ecto.Query.FromExpr{source: {source, _schema}},
          wheres: [%Ecto.Query.BooleanExpr{
            expr: {:==, [], [{{:., [], [{:&, [], [0]}, _pk_field]}, [], []}, {:^, [], [0]}]}
          }]
        },
        [pk]
      ) do
    key = key(adapter_opts, source, pk)

    fn tx ->
      :erlfdb.get(tx, key)
      |> :erlfdb.wait()
      |> decode_value()
      |> Enum.map(fn {_, x} -> x end)
      |> enlist()
    end
  end
  def all(_, query, _) do
    IO.inspect(Map.drop(query, [:__struct__]))
  end

  defp key(adapter_opts, source, pk) do
    # TODO: support non-binary pks
    Enum.join([source, pk], adapter_opts[:key_delimiter])
  end

  defp encode_value(fields), do: :erlang.term_to_binary(fields)

  defp decode_value(bin), do: :erlang.binary_to_term(bin)

  defp enlist(x), do: [x]
end
