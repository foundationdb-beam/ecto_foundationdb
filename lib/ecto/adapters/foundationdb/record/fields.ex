defmodule Ecto.Adapters.FoundationDB.Record.Fields do
  def get_pk_field!(schema) do
    # TODO: support composite primary key
    [pk_field] = schema.__schema__(:primary_key)
    pk_field
  end

  def pk_front(fields, pk_field) do
    pk = fields[pk_field]
    [{pk_field, pk}|Keyword.drop(fields, [pk_field])]
  end
end
