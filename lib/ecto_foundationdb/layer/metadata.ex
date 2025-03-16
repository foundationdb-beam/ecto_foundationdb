defmodule EctoFoundationDB.Layer.Metadata do
  @moduledoc false
  alias EctoFoundationDB.Indexer.Default
  alias EctoFoundationDB.Indexer.MaxValue
  alias EctoFoundationDB.Layer.Metadata.Cache
  alias EctoFoundationDB.Layer.Pack
  alias EctoFoundationDB.Layer.Tx
  alias EctoFoundationDB.Migration.SchemaMigration
  alias EctoFoundationDB.MigrationsPJ
  alias EctoFoundationDB.QueryPlan

  # @todo: Ideally we change this, but it's a breaking change
  @metadata_source "indexes"
  @max_version_name "version"
  @metadata_operation_failed {:erlfdb_error, 1020}

  @builtin_metadata %{
    SchemaMigration.source() => %{
      partial_indexes: [],
      other: [],
      indexes: [
        [
          id: @max_version_name,
          type: :index,
          indexer: MaxValue,
          source: SchemaMigration.source(),
          fields: [:version],
          options: []
        ]
      ]
    }
  }

  defstruct indexes: [], partial_indexes: [], other: []

  @type t() :: %__MODULE__{}

  def source(), do: @metadata_source

  def new(metadata_kwl) do
    new(%__MODULE__{}, metadata_kwl)
  end

  defp new(struct, []) do
    %__MODULE__{indexes: indexes, other: other} = struct
    %__MODULE__{struct | indexes: Enum.reverse(indexes), other: Enum.reverse(other)}
  end

  defp new(struct, [meta | metadata_kwl]) do
    %__MODULE__{indexes: indexes, other: other} = struct

    # Default type is index for historical reasons
    case meta[:type] || :index do
      :index ->
        new(%__MODULE__{struct | indexes: [meta | indexes]}, metadata_kwl)

      _ ->
        new(%__MODULE__{struct | other: [meta | other]}, metadata_kwl)
    end
  end

  def find_indexes_using_field(metadata, field) do
    %__MODULE__{indexes: indexes, partial_indexes: partial_indexes} = metadata

    indexes = indexes ++ for({idx, _} <- partial_indexes, do: idx)

    Enum.filter(indexes, fn idx -> field in idx[:fields] end)
  end

  @doc """
  Create an FDB kv to be stored in the schema_migrations source. This kv contains
  the information necessary to manage data objects' associated indexes.

  ## Examples

    iex> tenant = %EctoFoundationDB.Tenant{backend: EctoFoundationDB.Tenant.ManagedTenant}
    iex> {key, obj} = EctoFoundationDB.Layer.Metadata.new_index(tenant, "users", "users_name_index", [:name], [])
    iex> {EctoFoundationDB.Tenant.unpack(tenant, key), obj}
    {{"\\xFE", "indexes", "users", "users_name_index"}, [id: "users_name_index", type: :index, indexer: EctoFoundationDB.Indexer.Default, source: "users", fields: [:name], options: []]}

  """
  def new_index(tenant, source, index_name, index_fields, options) do
    metadata_key = metadata_key(tenant, source, index_name)

    idx = [
      id: index_name,
      type: :index,
      indexer: get_indexer(options),
      source: source,
      fields: index_fields,
      options: options
    ]

    {metadata_key, idx}
  end

  def metadata_key(tenant, source, index_name) do
    Pack.namespaced_pack(tenant, source(), source, ["#{index_name}"])
  end

  defp get_indexer(options) do
    case Keyword.get(options, :indexer) do
      nil -> Default
      module -> module
    end
  end

  def select_index(metadata, constraints) do
    %__MODULE__{indexes: indexes} = metadata

    case Enum.min(indexes, &choose_a_over_b_or_throw(&1, &2, constraints), fn -> nil end) do
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
  def transactional(tenant, source, fun) do
    transactional(tenant, %{cache: nil}, source, fun)
  end

  def transactional(tenant, %{cache: cache}, source, fun) do
    cache? = :enabled == (tenant.options[:metadata_cache] || :enabled)
    cache = if cache?, do: cache, else: nil

    Tx.transactional(tenant, fn tx ->
      tx_with_metadata_cache(tenant, tx, cache, source, fun)
    end)
  end

  def tx_with_metadata_cache(tenant, tx, cache, source, fun) do
    now = System.monotonic_time(:millisecond)
    cache_key = Cache.key(tenant, source)

    cache_item = Cache.lookup(cache, cache_key)

    {vsn, metadata, validator} = tx_metadata_get(tenant, tx, source, cache_item)

    new_cache_item = Cache.CacheItem.new(cache_key, vsn, metadata, now)

    Cache.update(cache, cache_item, new_cache_item)

    try do
      fun.(tx, metadata)
    after
      unless validator.() do
        :ets.delete(cache, cache_key)
        :erlang.error(@metadata_operation_failed)
      end
    end
  end

  defp tx_metadata_get(tenant, tx, source, cache_item) do
    case Map.get(@builtin_metadata, source, nil) do
      nil ->
        # Every transaction performs a 'get' on these two keys, which **does** have an impact
        # on performance. The cost of the 'get' is worth it because
        #
        # 1. the max_version allows us to cache the metadata in ets so that we don't have to read
        #    them as a blocking operation before doing actual index queries.
        # 2. The claim key allows us to implement concurrently created indexes. With it,
        #    we inspect the indexes that are partially created and use them accordingly.
        #
        # In both cases, we only wait for the result at the end of the rest of the transaction
        # which gives FDB the opportunity to select the keys in the background while the user's
        # more interesting work is being executed. If something is found to be violated at the end
        # of the transaction, the entire transaction is retried.
        max_version_future = MaxValue.get(tenant, tx, SchemaMigration.source(), @max_version_name)
        claim_future = :erlfdb.get(tx, MigrationsPJ.claim_key(tenant, source))

        case cache_item do
          nil ->
            tx_metadata_get_wait(tenant, tx, source, max_version_future, claim_future)

          _ ->
            tx_metadata_try_cache(cache_item, max_version_future, claim_future)
        end

      metadata ->
        {-1, struct(__MODULE__, metadata), fn -> true end}
    end
  end

  defp tx_metadata_try_cache(cache_item, max_version_future, claim_future) do
    cache_vsn = Cache.CacheItem.get_version(cache_item)

    vsn_validator = fn ->
      [max_version, claim] =
        [max_version_future, claim_future]
        |> :erlfdb.wait_for_all()

      MaxValue.decode(max_version) <= cache_vsn and
        :not_found == claim
    end

    {cache_vsn, Cache.CacheItem.get_metadata(cache_item), vsn_validator}
  end

  defp tx_metadata_get_wait(tenant, tx, source, max_version_future, claim_future) do
    {start_key, end_key} = metadata_range(tenant, source)

    metadata_future = :erlfdb.get_range(tx, start_key, end_key, wait: false)

    [max_version, claim, metadata] =
      :erlfdb.wait_for_all_interleaving(tx, [max_version_future, claim_future, metadata_future])

    metadata =
      metadata
      |> Enum.map(fn {_key, fdb_value} -> Pack.from_fdb_value(fdb_value) end)
      # high priority first
      |> Enum.sort(&(Keyword.get(&1, :priority, 0) > Keyword.get(&2, :priority, 0)))
      |> new()

    case get_partial_idxs(claim, source) do
      {claim_active?, []} ->
        {MaxValue.decode(max_version), metadata, fn -> not claim_active? end}

      {_, partial_idxs} ->
        # Adding a write conflict here allows writes to commit at a lower latency
        # at the expense of migration taking longer, because we're not letting the
        # migration job starve us out
        :erlfdb.add_write_conflict_key(tx, MigrationsPJ.claim_key(tenant, source))
        metadata = %__MODULE__{partial_indexes: partial_idxs}
        {MaxValue.decode(max_version), metadata, fn -> true end}
    end
  end

  defp metadata_range(tenant, source) do
    Pack.namespaced_range(tenant, source(), source, [])
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

        p_idx = {idx, _} ->
          if idx[:source] == source do
            [p_idx]
          else
            []
          end
      end

    claim_active? = length(partial_idxs) > 0

    partial_idxs =
      Enum.filter(partial_idxs, fn {idx, _} ->
        p_options = idx[:options]
        Keyword.get(p_options, :concurrently, true)
      end)

    {claim_active?, partial_idxs}
  end
end
