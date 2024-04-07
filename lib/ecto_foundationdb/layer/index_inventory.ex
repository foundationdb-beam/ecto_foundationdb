defmodule EctoFoundationDB.Layer.IndexInventory do
  @moduledoc false
  alias EctoFoundationDB.Indexer.Default
  alias EctoFoundationDB.Indexer.MaxValue
  alias EctoFoundationDB.Layer.Pack
  alias EctoFoundationDB.Layer.Tx
  alias EctoFoundationDB.Migration.SchemaMigration
  alias EctoFoundationDB.MigrationsPJ
  alias EctoFoundationDB.Options
  alias EctoFoundationDB.QueryPlan

  @index_inventory_source "\xFFindexes"
  @max_version_name "version"
  @idx_operation_failed {:erlfdb_error, 1020}

  def source(), do: @index_inventory_source

  def builtin_indexes() do
    migration_source = SchemaMigration.source()

    %{
      migration_source => [
        [
          id: @max_version_name,
          indexer: MaxValue,
          source: migration_source,
          fields: [:version],
          options: []
        ]
      ]
    }
  end

  @doc """
  Create an FDB kv to be stored in the schema_migrations source. This kv contains
  the information necessary to manage data objects' associated indexes.

  ## Examples

    iex> {key, obj} = EctoFoundationDB.Layer.IndexInventory.new_index("users", "users_name_index", [:name], [])
    iex> {:erlfdb_tuple.unpack(key), obj}
    {{"\\xFE", "\\xFFindexes", "users", "users_name_index"}, [id: "users_name_index", indexer: EctoFoundationDB.Indexer.Default, source: "users", fields: [:name], options: []]}

  """
  def new_index(source, index_name, index_fields, options) do
    inventory_key = inventory_key(source, index_name)

    idx = [
      id: index_name,
      indexer: get_indexer(options),
      source: source,
      fields: index_fields,
      options: options
    ]

    {inventory_key, idx}
  end

  def inventory_key(source, index_name) do
    Pack.namespaced_pack(source(), source, ["#{index_name}"])
  end

  def special_key(name) do
    Pack.namespaced_pack(source(), name, [])
  end

  defp get_indexer(options) do
    case Keyword.get(options, :indexer) do
      nil -> Default
      module -> module
    end
  end

  def select_index(idxs, constraints) do
    case Enum.min(idxs, &choose_a_over_b_or_throw(&1, &2, constraints), fn -> nil end) do
      nil -> nil
      idx -> if(sufficient?(idx, constraints), do: idx, else: nil)
    end
  catch
    {:best, idx} -> idx
  end

  def arrange_constraints(constraints, idx) do
    constraints = for op <- constraints, do: {op.field, op}

    for(field <- idx[:fields], do: constraints[field])
    |> Enum.filter(fn
      nil -> false
      _ -> true
    end)
  end

  defp sufficient?(idx, constraints) when is_list(idx) and is_list(constraints) do
    fields = idx[:fields] |> Enum.into(MapSet.new())
    where_fields_list = for(op <- constraints, do: op.field)
    where_fields = Enum.into(where_fields_list, MapSet.new())
    sufficient?(fields, where_fields)
  end

  defp sufficient?(fields, where_fields) do
    0 == MapSet.difference(where_fields, fields) |> MapSet.size()
  end

  defp choose_a_over_b_or_throw(idx_a, idx_b, constraints) do
    fields_a = idx_a[:fields] |> Enum.into(MapSet.new())
    fields_b = idx_b[:fields] |> Enum.into(MapSet.new())
    where_fields_list = for(op <- constraints, do: op.field)
    where_fields = Enum.into(where_fields_list, MapSet.new())

    overlap_a = MapSet.intersection(where_fields, fields_a) |> MapSet.size()
    overlap_b = MapSet.intersection(where_fields, fields_b) |> MapSet.size()

    match_a? = overlap_a == MapSet.size(fields_a) and overlap_a == MapSet.size(where_fields)
    match_b? = overlap_b == MapSet.size(fields_b) and overlap_b == MapSet.size(where_fields)

    exact_match_short_circuit(match_a?, idx_a, match_b?, idx_b)

    # Most indexes will be Default indexes, so we can optimize for that case. Default index allows 0 or 1
    # Between ops, and it always must be the last field in the index.
    between_fields = for %QueryPlan.Between{field: field} <- constraints, do: field

    final_between_a? =
      length(between_fields) <= 1 and
        Enum.slice(idx_a[:fields], 0, length(where_fields_list)) ==
          where_fields_list

    final_between_b? =
      length(between_fields) <= 1 and
        Enum.slice(idx_b[:fields], 0, length(where_fields_list)) ==
          where_fields_list

    default_index_short_circuit(
      match_a?,
      final_between_a?,
      idx_a,
      match_b?,
      final_between_b?,
      idx_b
    )

    # index_sufficient is true when the where fields are a subset of the index fields
    index_sufficient_a = sufficient?(fields_a, where_fields)
    index_sufficient_b = sufficient?(fields_b, where_fields)

    choose_a_over_b_partial(
      index_sufficient_a,
      final_between_a?,
      overlap_a,
      fields_a,
      index_sufficient_b,
      final_between_b?,
      overlap_b,
      fields_b
    )
  end

  defp exact_match_short_circuit(match_a?, idx_a, match_b?, idx_b)
  defp exact_match_short_circuit(true, idx_a, false, _idx_b), do: throw({:best, idx_a})
  defp exact_match_short_circuit(false, _idx_a, true, idx_b), do: throw({:best, idx_b})
  defp exact_match_short_circuit(_, _idx_a, _, _idx_b), do: nil

  defp default_index_short_circuit(
         match_a?,
         final_between_a?,
         indx_a,
         match_b?,
         final_between_b?,
         idx_b
       )

  defp default_index_short_circuit(true, true, idx_a, true, false, _idx_b),
    do: throw({:best, idx_a})

  defp default_index_short_circuit(true, false, _idx_a, true, true, idx_b),
    do: throw({:best, idx_b})

  defp default_index_short_circuit(true, _, idx_a, true, _, _idx_b), do: throw({:best, idx_a})
  defp default_index_short_circuit(_, _, _idx_a, _, _, _idx_b), do: nil

  defp choose_a_over_b_partial(
         index_sufficient_a,
         final_between_a?,
         overlap_a,
         fields_a,
         index_sufficient_b,
         final_between_b?,
         overlap_b,
         fields_b
       )

  defp choose_a_over_b_partial(true, _, _, _, false, _, _, _), do: true
  defp choose_a_over_b_partial(false, _, _, _, true, _, _, _), do: false

  # Then, check to see if the final field is a between constraint. This
  # optimizes for Default indexes
  defp choose_a_over_b_partial(_, true, _, _, _, false, _, _), do: true
  defp choose_a_over_b_partial(_, false, _, _, _, true, _, _), do: false

  # Finally, check for the best partial matches
  defp choose_a_over_b_partial(_, _, overlap_a, fields_a, _, _, overlap_b, fields_b) do
    constraints_partially_determined_a = overlap_a / MapSet.size(fields_a)

    constraints_partially_determined_b = overlap_b / MapSet.size(fields_b)

    constraints_partially_determined_a > constraints_partially_determined_b
  end

  @doc """
  Executes function within a transaction, while also supplying the indexes currently
  existing for the schema.

  This function uses the Ecto cache and clever use of FDB constructs to guarantee
  that the cache is consistent with transactional semantics.
  """
  def transactional(db_or_tenant, %{cache: cache, opts: adapter_opts}, source, fun) do
    cache? = :enabled == Options.get(adapter_opts, :idx_cache)
    cache_key = {__MODULE__, db_or_tenant, source}

    Tx.transactional(db_or_tenant, fn tx ->
      tx_with_idxs_cache(tx, cache?, cache, adapter_opts, source, cache_key, fun)
    end)
  end

  defp tx_with_idxs_cache(tx, cache?, cache, adapter_opts, source, cache_key, fun) do
    now = System.monotonic_time(:millisecond)

    {_, {cvsn, cidxs}, ts} = cache_lookup(cache?, cache, cache_key, now)

    {vsn, idxs, partial_idxs, validator} = tx_idxs(tx, adapter_opts, source, {cvsn, cidxs})

    cache_update(cache?, cache, cache_key, {cvsn, cidxs}, {vsn, idxs}, partial_idxs, ts, now)

    try do
      fun.(tx, idxs, partial_idxs)
    after
      unless validator.() do
        :ets.delete(cache, cache_key)
        :erlang.error(@idx_operation_failed)
      end
    end
  end

  defp tx_idxs(tx, adapter_opts, source, cache_val) do
    case Map.get(builtin_indexes(), source, nil) do
      nil ->
        tx_idxs_get(tx, adapter_opts, source, cache_val)

      idxs ->
        {-1, idxs, [], fn -> true end}
    end
  end

  defp tx_idxs_get(tx, adapter_opts, source, {vsn, idxs}) do
    # Every transaction performs a 'get' on these two keys, which **does** have an impact
    # on performance. The cost of the 'get' is worth it because
    #
    # 1. the max_version allows us to cache the idxs in ets so that we don't have to read
    #    them as a blocking operation before doing actual index queries.
    # 2. The claim key allows us to implement concurrently created indexes. With it,
    #    we inspect the indexes that are partially created and use them accordingly.
    #
    # In both cases, we only wait for the result at the end of the rest of the transaction
    # which gives FDB the opportunity to select the keys in the background while the user's
    # more interesting work is being excuted. If something is found to be violated at the end
    # of the transaction, the entire transaction is retried.
    max_version_future = MaxValue.get(tx, SchemaMigration.source(), @max_version_name)
    claim_future = :erlfdb.get(tx, MigrationsPJ.claim_key(source))

    case idxs do
      idxs when is_list(idxs) ->
        tx_idxs_try_cache({vsn, idxs}, max_version_future, claim_future)

      _idxs ->
        tx_idxs_get_wait(tx, adapter_opts, source, max_version_future, claim_future)
    end
  end

  defp tx_idxs_try_cache({vsn, idxs}, max_version_future, claim_future) do
    vsn_validator = fn ->
      [max_version, claim] =
        [max_version_future, claim_future]
        |> :erlfdb.wait_for_all()

      MaxValue.decode(max_version) <= vsn and
        :not_found == claim
    end

    {vsn, idxs, [], vsn_validator}
  end

  defp tx_idxs_get_wait(tx, _adapter_opts, source, max_version_future, claim_future) do
    {start_key, end_key} = idx_range(source)

    idxs =
      tx
      |> :erlfdb.get_range(start_key, end_key)
      |> :erlfdb.wait()
      |> Enum.map(fn {_, fdb_value} -> Pack.from_fdb_value(fdb_value) end)
      # high priority first
      |> Enum.sort(&(Keyword.get(&1, :priority, 0) > Keyword.get(&2, :priority, 0)))

    max_version = :erlfdb.wait(max_version_future)

    claim = :erlfdb.wait(claim_future)

    case get_partial_idxs(claim, source) do
      {claim_active?, []} ->
        {MaxValue.decode(max_version), idxs, [], fn -> not claim_active? end}

      {_, partial_idxs} ->
        # Adding a write conflict here allows writes to commit at a lower latency
        # at the expense of migration taking longer, becuase we're not letting the
        # migration job starve us out
        :erlfdb.add_write_conflict_key(tx, MigrationsPJ.claim_key(source))
        {MaxValue.decode(max_version), idxs, partial_idxs, fn -> true end}
    end
  end

  defp idx_range(source) do
    Pack.namespaced_range(source(), source, [])
  end

  defp get_partial_idxs(:not_found, _), do: {false, []}

  defp get_partial_idxs(claim, source) do
    idx_being_created =
      Pack.from_fdb_value(claim)
      |> MigrationsPJ.get_idx_being_created()

    partial_idxs =
      case idx_being_created do
        nil ->
          []

        pidx = {partial_idx, _} ->
          if partial_idx[:source] == source do
            [pidx]
          else
            []
          end
      end

    claim_active? = length(partial_idxs) > 0

    partial_idxs =
      Enum.filter(partial_idxs, fn {idx, _} ->
        pidx_options = idx[:options]
        Keyword.get(pidx_options, :concurrently, true)
      end)

    {claim_active?, partial_idxs}
  end

  defp cache_lookup(cache?, cache, cache_key, now) do
    case {cache?, :ets.lookup(cache, cache_key)} do
      {true, [item]} ->
        item

      _ ->
        {cache_key, {-1, nil}, now}
    end
  end

  defp cache_update(cache?, cache, cache_key, {cvsn, cidxs}, {vsn, idxs}, [], ts, now) do
    cond do
      cache? and vsn >= 0 and {vsn, idxs} != {cvsn, cidxs} ->
        :ets.insert(cache, {cache_key, {vsn, idxs}, System.monotonic_time(:millisecond)})

      cache? and cvsn >= 0 ->
        diff = now - ts

        try do
          :ets.update_counter(cache, cache_key, {3, diff})
        catch
          _, _ ->
            # Someone else removed the cache entry
            :ok
        end

      true ->
        :ok
    end
  end

  defp cache_update(
         _cache?,
         cache,
         cache_key,
         {_cvsn, _cidxs},
         {_vsn, _idxs},
         _partial_idxs,
         _ts,
         _now
       ) do
    # partial idxs cannot be cached
    :ets.delete(cache, cache_key)
  end
end
