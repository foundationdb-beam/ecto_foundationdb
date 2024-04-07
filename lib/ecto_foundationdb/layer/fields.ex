defmodule EctoFoundationDB.Layer.Fields do
  @moduledoc false

  @doc """
  Ecto provides a compiled set of 'select's. We simply pull the field names out

  ## Examples

    iex> EctoFoundationDB.Layer.Fields.parse_select_fields([{{:., [], [{:&, [], [0]}, :a]}, [], []}])
    [:a]

  """
  def parse_select_fields(select_fields) do
    select_fields
    |> Enum.map(fn {{:., _, [{:&, [], [0]}, field]}, [], []} -> field end)
  end

  @doc """
  Given a Keyword of key-value pairs, arrange them in the order of the passed-in
  fields.

  ## Examples

    iex> EctoFoundationDB.Layer.Fields.arrange([b: 1, c: 2, a: 0], [:a, :b])
    [a: 0, b: 1]

    iex> EctoFoundationDB.Layer.Fields.arrange([b: 1, c: 2, a: 0], [])
    [b: 1, c: 2, a: 0]

  """
  def arrange(fields, []) do
    fields
  end

  def arrange(fields, field_names) do
    Enum.map(field_names, fn field_name -> {field_name, fields[field_name]} end)
  end

  @doc """
  Ecto expects data to be returned from queries as just a list of values. This
  function removes the field names from each.

  ## Examples

    iex> EctoFoundationDB.Layer.Fields.strip_field_names_for_ecto([[a: 0, b: 1, c: 2]])
    [[0,1,2]]

  """
  def strip_field_names_for_ecto(entries) do
    Enum.map(entries, &Keyword.values/1)
  end

  @doc """
  Gets the name of the primary key field from the schema.
  """
  def get_pk_field!(schema) do
    [pk_field] = schema.__schema__(:primary_key)
    pk_field
  end

  @doc """
  Brings the given key-value pair to the front of the Keyword

  ## Examples

    iex> EctoFoundationDB.Layer.Fields.to_front([a: 0, b: 1, c: 2], :c)
    [c: 2, a: 0, b: 1]

  """
  def to_front(kw = [{first_key, _} | _], key) do
    if first_key == key do
      kw
    else
      val = kw[key]
      [{key, val} | Keyword.delete(kw, key)]
    end
  end
end
