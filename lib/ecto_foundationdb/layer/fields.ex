defmodule EctoFoundationDB.Layer.Fields.CompositePK do
  @moduledoc false
  # Primary key value for a schema with more than one primary key field.
  # Never produced for a single-field key, so those keys encode unchanged.
  @enforce_keys [:values]
  defstruct @enforce_keys
end

defmodule EctoFoundationDB.Layer.Fields do
  @moduledoc false

  alias EctoFoundationDB.Layer.Fields.CompositePK

  @doc """
  Ecto provides a compiled set of 'select's. We simply pull the field names out

  ## Examples

    iex> EctoFoundationDB.Layer.Fields.parse_select_fields([{{:., [], [{:&, [], [0]}, :a]}, [], []}])
    [:a]

  """
  def parse_select_fields(select_fields) do
    select_fields
    |> Enum.map(fn
      {{:., _, [{:&, [], [0]}, field]}, [], []} ->
        field

      atom when is_atom(atom) ->
        atom
    end)
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

    iex> EctoFoundationDB.Layer.Fields.strip_field_names_for_ecto([[a: 0, b: 1, c: 2]]) |> Enum.to_list()
    [[0,1,2]]

  """
  def strip_field_names_for_ecto(entries) do
    Stream.map(entries, &Keyword.values/1)
  end

  @doc """
  Gets the primary key field names, in declared order. That order decides the
  order of the elements in the FDB key tuple, and so the sort order.
  """
  def get_pk_fields!(schema) do
    case schema.__schema__(:primary_key) do
      [] ->
        raise ArgumentError, """
        #{inspect(schema)} has no primary key. EctoFoundationDB requires one.
        """

      pk_fields ->
        pk_fields
    end
  end

  @doc """
  True when the schema declares more than one primary key field.
  """
  def composite_pk?(schema), do: length(get_pk_fields!(schema)) > 1

  @doc """
  Builds the primary key value from a `Keyword` or `Map` of field values.

  A single-field key yields the bare value, unchanged. A composite key yields
  a `CompositePK` struct, which `Pack.primary_codec/3` splices into the key
  tuple. A struct, not a tagged tuple, so it cannot collide with a user's own
  primary key value.
  """
  def get_pk_value!(schema, source) do
    case get_pk_fields!(schema) do
      [pk_field] ->
        fetch_field(source, pk_field)

      pk_fields ->
        %CompositePK{values: Enum.map(pk_fields, &fetch_field(source, &1))}
    end
  end

  defp fetch_field(source, field) when is_list(source), do: source[field]
  defp fetch_field(source, field), do: Map.get(source, field)

  @doc """
  Brings the given key-value pair to the front of the Keyword

  A list of keys brings all of them to the front, in the order given.

  ## Examples

    iex> EctoFoundationDB.Layer.Fields.to_front([a: 0, b: 1, c: 2], :c)
    [c: 2, a: 0, b: 1]

    iex> EctoFoundationDB.Layer.Fields.to_front([a: 0, b: 1, c: 2], [:b, :c])
    [b: 1, c: 2, a: 0]

  """
  def to_front(kw, keys) when is_list(keys) do
    keys
    |> Enum.reverse()
    |> Enum.reduce(kw, fn key, acc -> to_front(acc, key) end)
  end

  def to_front(kw = [{first_key, _} | _], key) do
    if first_key == key do
      kw
    else
      val = kw[key]
      [{key, val} | Keyword.delete(kw, key)]
    end
  end
end
