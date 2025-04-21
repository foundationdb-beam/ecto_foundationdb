defmodule EctoFoundationDB.Indexer.Default do
  @moduledoc false
  alias EctoFoundationDB.Exception.Unsupported
  alias EctoFoundationDB.Indexer
  alias EctoFoundationDB.Layer.Pack
  alias EctoFoundationDB.Layer.PrimaryKVCodec
  alias EctoFoundationDB.QueryPlan
  alias EctoFoundationDB.Schema
  alias EctoFoundationDB.Tenant

  @behaviour Indexer

  def indexkey_encoder(_left_or_right, _types, [], acc), do: Enum.reverse(acc)

  def indexkey_encoder(
        left_or_right,
        types,
        [%QueryPlan.Equal{param: param, field: field} | rest],
        acc
      ) do
    val = indexkey_encoder(param, types[field])
    indexkey_encoder(left_or_right, types, rest, [val | acc])
  end

  def indexkey_encoder(:left, types, [%QueryPlan.Between{param_left: nil} | rest], acc) do
    indexkey_encoder(:left, types, rest, acc)
  end

  def indexkey_encoder(
        :left,
        types,
        [
          %QueryPlan.Between{
            param_left: param,
            field: field
          }
          | rest
        ],
        acc
      ) do
    type = types[field]

    if not allows_between?(type) do
      raise Unsupported, """
      FoundationDB Adapter does not support Between queries on #{type}.
      """
    end

    val = indexkey_encoder(param, type)
    indexkey_encoder(:left, types, rest, [val | acc])
  end

  def indexkey_encoder(:right, types, [%QueryPlan.Between{param_right: nil} | rest], acc) do
    indexkey_encoder(:right, types, rest, acc)
  end

  def indexkey_encoder(
        :right,
        types,
        [
          %QueryPlan.Between{
            param_right: param,
            field: field
          }
          | rest
        ],
        acc
      ) do
    type = types[field]

    if not allows_between?(type) do
      raise Unsupported, """
      FoundationDB Adapter does not support Between queries on #{type}.
      """
    end

    val = indexkey_encoder(param, type)
    indexkey_encoder(:right, types, rest, [val | acc])
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

  def indexkey_encoder(x, nil) when is_binary(x), do: x
  def indexkey_encoder(x, nil) when is_boolean(x), do: x
  def indexkey_encoder(x, nil) when is_atom(x), do: "#{x}"
  def indexkey_encoder(x, nil) when is_number(x), do: x

  def indexkey_encoder(x, _) do
    :erlang.term_to_binary(x)
  end

  def allows_between?(nil), do: true
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
  def create_range(tenant, idx) do
    source = idx[:source]
    Pack.primary_range(tenant, source)
  end

  @impl true
  def drop_ranges(tenant, idx) do
    source = idx[:source]
    index_name = idx[:id]
    [Pack.default_index_range(tenant, source, index_name)]
  end

  @impl true
  def create(tenant, tx, idx, schema, {start_key, end_key}, limit) do
    keys =
      tx
      |> :erlfdb.get_range(start_key, end_key, limit: limit, wait: true)
      |> Enum.map(fn {fdb_key, fdb_value} ->
        # @todo: this won't work for multikey objects, need to stream_decode.. but I'm not guaranteed to have
        # the full set of keys.
        kv_codec =
          PrimaryKVCodec.new(fdb_key)
          |> PrimaryKVCodec.with_unpacked_tuple(tenant)

        entry =
          get_index_entry(
            tenant,
            idx,
            schema,
            {kv_codec, Pack.from_fdb_value(fdb_value)}
          )

        set_index_entry(tx, entry)
        fdb_key
      end)

    case keys do
      [] ->
        {0, {end_key, end_key}}

      _ ->
        last_key = List.last(keys)
        next_key = :erlfdb_key.strinc(last_key)

        {length(keys), {next_key, end_key}}
    end
  end

  @impl true
  def set(tenant, tx, idx, schema, kv) do
    entry = get_index_entry(tenant, idx, schema, kv)
    set_index_entry(tx, entry)
    :ok
  end

  # FDB API doesn't support setting the versionstamp on both the key and value in the
  # same transaction. So, we must choose either the key or the value of the index entry.
  # A natural choice is to use the value to have the versionstamp so that
  # get_mapped_range works correctly. However, then we lose the ability to manage the index
  # on updates and clears. Therefore, we must put the versionstamp in the index_key and
  # which forces our mapper to inspect both the key and value to extract the pk.
  #
  # This means that the value portion of the index kv will always be written with an
  # incomplete versionstamp. Maybe someday FDB will support setting both the key and
  # value, and this can be cleaned up
  defp set_index_entry(tx, {index_key, kv_codec = %PrimaryKVCodec{}, false, true}) do
    :erlfdb.set(tx, index_key, kv_codec.packed)
  end

  defp set_index_entry(tx, {index_key, kv_codec = %PrimaryKVCodec{}, true, true}) do
    :erlfdb.set_versionstamped_key(tx, index_key, kv_codec.packed)
  end

  defp set_index_entry(tx, {index_key, index_object, true, false}) when is_binary(index_object) do
    :erlfdb.set_versionstamped_key(tx, index_key, index_object)
  end

  defp set_index_entry(tx, {index_key, index_object, false, false})
       when is_binary(index_object) do
    :erlfdb.set(tx, index_key, index_object)
  end

  @impl true
  def clear(tenant, tx, idx, schema, kv) do
    {index_key, _index_object, _, _} = get_index_entry(tenant, idx, schema, kv)

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
    types = if is_nil(plan.schema), do: nil, else: Schema.field_types(plan.schema, fields)

    left_values = indexkey_encoder(:left, types, constraints, [])
    right_values = indexkey_encoder(:right, types, constraints, [])

    {left_range_start, left_range_end} =
      Pack.default_index_range(
        plan.tenant,
        plan.source,
        idx[:id],
        length(fields),
        left_values
      )

    {right_range_start, right_range_end} =
      Pack.default_index_range(
        plan.tenant,
        plan.source,
        idx[:id],
        length(fields),
        right_values
      )

    {start_key, end_key} =
      case List.last(constraints) do
        %QueryPlan.Equal{} ->
          {left_range_start, right_range_end}

        between = %QueryPlan.Between{} ->
          sk = if between.inclusive_left?, do: left_range_start, else: left_range_end
          ek = if between.inclusive_right?, do: right_range_end, else: right_range_start
          {sk, ek}
      end

    start_key = options[:start_key] || start_key

    if Keyword.get(idx[:options], :mapped?, true) do
      {start_key, end_key, mapper(plan.tenant, length(fields))}
    else
      {start_key, end_key}
    end
  end

  # Computes the key-value pair that defines the index indirection
  #
  # Key:
  # { (tenant_prefix,) <<0xFE>>, source, "i", index_name, idx_len, index_values..., pk }
  #
  # Value:
  #   mapped? == true:
  #     primary_write_key =>
  #        { (tenant_prefix,) <<0xFD>>, source, "d", pk }
  #
  #   mapped? == false:
  #     data_object
  #
  #
  # Note: pk is always first. See insert and update paths
  defp get_index_entry(tenant, idx, schema, {kv_codec, data_object = [{pk_field, pk_value} | _]}) do
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

    vs? = PrimaryKVCodec.vs?(kv_codec)
    mapped? = Keyword.get(index_options, :mapped?, true)

    index_key =
      if vs? do
        Pack.default_index_pack_vs(
          tenant,
          source,
          index_name,
          length(index_fields),
          index_values,
          pk_value
        )
      else
        Pack.default_index_pack(
          tenant,
          source,
          index_name,
          length(index_fields),
          index_values,
          pk_value
        )
      end

    if mapped? do
      {index_key, kv_codec, vs?, mapped?}
    else
      # unmapped index values do not support key/value splitting
      {index_key, Pack.to_fdb_value(data_object), vs?, mapped?}
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

  # See key-value design in get_index_entry
  defp mapper(tenant, idx_len) do
    offset_fun = fn offset ->
      [
        # prefix
        "{V[#{offset}]}",

        # source
        "{V[#{offset + 1}]}",

        # namespace
        "{V[#{offset + 2}]}",

        # pk: get it from the index_key because the versionstamp lives there, not in the value
        "{K[#{offset + 5 + idx_len}]}",

        # rest
        "{...}"
      ]
    end

    Tenant.extend_tuple(tenant, offset_fun)
  end
end
