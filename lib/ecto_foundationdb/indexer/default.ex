defmodule EctoFoundationDB.Indexer.Default do
  @moduledoc false
  alias EctoFoundationDB.Exception.Unsupported
  alias EctoFoundationDB.Indexer
  alias EctoFoundationDB.Layer.Pack
  alias EctoFoundationDB.QueryPlan
  alias EctoFoundationDB.Schema

  @behaviour Indexer

  def indexkey_encoder(_left_or_right, types, %QueryPlan.Equal{param: param, field: field}) do
    indexkey_encoder(param, types[field])
  end

  def indexkey_encoder(:left, types, %QueryPlan.Between{
        param_left: param,
        field: field,
        inclusive_left?: inclusive_left?
      }) do
    type = types[field]

    if not allows_between?(type) do
      raise Unsupported, """
      FoundationDB Adapter does not support Between queries on #{type}.
      """
    end

    val = indexkey_encoder(param, type)
    if inclusive_left?, do: val, else: :erlfdb_key.strinc(val)
  end

  def indexkey_encoder(:right, types, %QueryPlan.Between{
        param_right: param,
        field: field,
        inclusive_right?: inclusive_right?
      }) do
    type = types[field]

    if not allows_between?(type) do
      raise Unsupported, """
      FoundationDB Adapter does not support Between queries on #{type}.
      """
    end

    val = indexkey_encoder(param, type)
    if inclusive_right?, do: :erlfdb_key.strinc(val), else: val
  end

  @doc """

  ## Examples

    iex> EctoFoundationDB.Indexer.Default.indexkey_encoder("an-example-key", :string)
    "an-example-key"

    iex> EctoFoundationDB.Indexer.Default.indexkey_encoder(~N[2024-03-01 12:34:56], :naive_datetime_usec)
    "20240301T123456.000000"

  """
  def indexkey_encoder(nil, _), do: :null
  def indexkey_encoder(x, :id), do: x
  def indexkey_encoder(x, :binary_id), do: x
  def indexkey_encoder(x, :integer), do: x
  def indexkey_encoder(x, :float), do: x
  def indexkey_encoder(x, :boolean), do: x
  def indexkey_encoder(x, :string), do: x
  def indexkey_encoder(x, :binary), do: x
  def indexkey_encoder(x, :date), do: x |> Date.to_iso8601(:basic)
  def indexkey_encoder(x, :time), do: x |> Time.add(0, :microsecond) |> Time.to_iso8601(:basic)

  def indexkey_encoder(x, :time_usec),
    do: x |> Time.add(0, :microsecond) |> Time.to_iso8601(:basic)

  def indexkey_encoder(x, :naive_datetime),
    do: x |> NaiveDateTime.add(0, :microsecond) |> NaiveDateTime.to_iso8601(:basic)

  def indexkey_encoder(x, :naive_datetime_usec),
    do: x |> NaiveDateTime.add(0, :microsecond) |> NaiveDateTime.to_iso8601(:basic)

  def indexkey_encoder(x, :utc_datetime),
    do: x |> DateTime.add(0, :microsecond) |> DateTime.to_iso8601(:basic)

  def indexkey_encoder(x, :utc_datetime_usec),
    do: x |> DateTime.add(0, :microsecond) |> DateTime.to_iso8601(:basic)

  def indexkey_encoder(x, _) do
    :erlang.term_to_binary(x)
  end

  def allows_between?(:id), do: true
  def allows_between?(:binary_id), do: true
  def allows_between?(:integer), do: true
  def allows_between?(:float), do: true
  def allows_between?(:boolean), do: true
  def allows_between?(:string), do: true
  def allows_between?(:binary), do: true
  def allows_between?(:date), do: true
  def allows_between?(:time), do: true
  def allows_between?(:time_usec), do: true
  def allows_between?(:naive_datetime), do: true
  def allows_between?(:naive_datetime_usec), do: true
  def allows_between?(:utc_datetime), do: true
  def allows_between?(:utc_datetime_usec), do: true
  def allows_between?(_), do: false

  @impl true
  def create_range(idx) do
    source = idx[:source]
    Pack.primary_range(source)
  end

  @impl true
  def create(tx, idx, schema, {start_key, end_key}, limit) do
    keys =
      tx
      |> :erlfdb.get_range(start_key, end_key, limit: limit)
      |> :erlfdb.wait()
      |> Enum.map(fn {fdb_key, fdb_value} ->
        {index_key, index_object} =
          get_index_entry(idx, schema, {fdb_key, Pack.from_fdb_value(fdb_value)})

        :erlfdb.set(tx, index_key, index_object)
        fdb_key
      end)

    {length(keys), {List.last(keys), end_key}}
  end

  @impl true
  def set(tx, idx, schema, kv) do
    {index_key, index_object} = get_index_entry(idx, schema, kv)

    :erlfdb.set(tx, index_key, index_object)
    :ok
  end

  @impl true
  def clear(tx, idx, schema, kv) do
    {index_key, _index_object} = get_index_entry(idx, schema, kv)

    :erlfdb.clear(tx, index_key)
    :ok
  end

  @impl true
  def range(_idx, %QueryPlan{constraints: [%QueryPlan.None{}]}, _options) do
    raise Unsupported, """
    FoundationDB Adapter does not support empty where clause on an index. In fact, this code path should not be reachable.
    """
  end

  def range(idx, plan = %QueryPlan{constraints: constraints}, options) do
    :ok = assert_constraints(idx[:fields], constraints)
    fields = idx[:fields]
    types = Schema.field_types(plan.schema, fields)

    left_values = for op <- constraints, do: indexkey_encoder(:left, types, op)
    right_values = for op <- constraints, do: indexkey_encoder(:right, types, op)

    start_key = Pack.default_index_pack(plan.source, idx[:id], length(fields), left_values, nil)
    end_key = Pack.default_index_pack(plan.source, idx[:id], length(fields), right_values, nil)

    end_key =
      case List.last(constraints) do
        %QueryPlan.Equal{} ->
          :erlfdb_key.strinc(end_key)

        _ ->
          end_key
      end

    start_key = options[:start_key] || start_key

    if Keyword.get(idx[:options], :mapped?, true) do
      {start_key, end_key, Pack.primary_mapper()}
    else
      {start_key, end_key}
    end
  end

  # Note: pk is always first. See insert and update paths
  defp get_index_entry(idx, schema, {fdb_key, data_object = [{pk_field, pk_value} | _]}) do
    index_name = idx[:id]
    index_fields = idx[:fields]
    index_options = idx[:options]
    source = idx[:source]

    index_fields = index_fields -- [pk_field]

    types = Schema.field_types(schema, index_fields)

    index_values =
      for idx_field <- index_fields do
        indexkey_encoder(Keyword.get(data_object, idx_field), types[idx_field])
      end

    index_key =
      Pack.default_index_pack(source, index_name, length(index_fields), index_values, pk_value)

    if Keyword.get(index_options, :mapped?, true) do
      {index_key, fdb_key}
    else
      {index_key, Pack.to_fdb_value(data_object)}
    end
  end

  defp assert_constraints([field | fields], [%QueryPlan.Equal{field: eq_field} | constraints]) do
    if field == eq_field do
      assert_constraints(fields, constraints)
    else
      raise Unsupported, """
      FoundationDB Adapter Default Index query mismatch: You must provide equals clauses for the first set of fields.
      """
    end
  end

  defp assert_constraints([field | _fields], [%QueryPlan.Between{field: bt_field}]) do
    if field == bt_field do
      :ok
    else
      raise Unsupported, """
      FoundationDB Adapter Default Index query mismatch. The Between clause must match the next field in the index after any Equal clauses.
      """
    end
  end

  defp assert_constraints([], _) do
    :ok
  end

  defp assert_constraints(_fields, _constraints) do
    raise Unsupported, """
    FoundationDB Adapter Default Index query mismatch. You must provide Equals and Between clauses such that the Equals clauses match
    the beginning set of index fields, and the Between clause matches the next field in the index. If there is no Between clause, then
    all fields must match the Equals clauses.
    """
  end

  # Future development notes:
  # This is the beginning of supporting creation of indexes from other indexes
  # (`write_primary: false` and `mapped?: false`). But the rest of the implementation
  # is not straightforward.
  #   1. For example when a `set/4` comes in, if the next index is mapped, then we
  #      need to compute the key for the `:from` index, but we don't have the infomation
  #      to do so.
  #   2. During migrations, partial_idxs are managed in the Indexer and they suffer the
  #       same problem as above.
  #
  # case options[:from] do
  #   nil ->
  #     Pack.primary_range(source)

  #   from ->
  #     Pack.default_index_range(source, from)
  # end
end
