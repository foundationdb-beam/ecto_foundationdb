defmodule EctoFoundationDB.Layer.Metadata do
  @moduledoc false
  alias EctoFoundationDB.Future
  alias EctoFoundationDB.Indexer
  alias EctoFoundationDB.Layer.Metadata.Cache
  alias EctoFoundationDB.Layer.MetadataVersion
  alias EctoFoundationDB.Layer.Pack
  alias EctoFoundationDB.Layer.Tx
  alias EctoFoundationDB.Migration.SchemaMigration
  alias EctoFoundationDB.MigrationsPJ
  alias EctoFoundationDB.QueryPlan

  @metadata_source "indexes"
  @metadata_operation_failed {:erlfdb_error, 1020}

  @app_metadata_version_idx [
    id: "version",
    type: :index,
    indexer: Indexer.MDVAppVersion,
    source: SchemaMigration.source(),
    fields: [:version],
    options: []
  ]

  @builtin_metadata %{
    SchemaMigration.source() => %{
      partial_indexes: [],
      other: [],
      indexes: [@app_metadata_version_idx]
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
    %{struct | indexes: Enum.reverse(indexes), other: Enum.reverse(other)}
  end

  defp new(struct, [meta | metadata_kwl]) do
    %__MODULE__{indexes: indexes, other: other} = struct

    # Default type is index for historical reasons
    case meta[:type] || :index do
      :index ->
        new(%{struct | indexes: [meta | indexes]}, metadata_kwl)

      _ ->
        new(%{struct | other: [meta | other]}, metadata_kwl)
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
      nil -> Indexer.Default
      module -> module
    end
  end

  def select_index(metadata, constraints, ordering) do
    %__MODULE__{indexes: indexes} = metadata

    case Enum.min(indexes, &choose_a_over_b_or_throw(&1, &2, constraints, ordering), fn -> nil end) do
      nil -> nil
      idx -> if(sufficient?(idx, constraints), do: idx, else: nil)
    end
  catch
    {:best, idx} -> idx
  end

  def arrange_constraints(constraints = [%QueryPlan.None{}], _idx) do
    constraints
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
    where_fields_list = get_fields_list(constraints, [])
    where_fields = Enum.into(where_fields_list, MapSet.new())
    sufficient?(fields, where_fields)
  end

  defp sufficient?(fields, where_fields) do
    0 == MapSet.difference(where_fields, fields) |> MapSet.size()
  end

  defp choose_a_over_b_or_throw(idx_a, idx_b, constraints, ordering) do
    fields_a = idx_a[:fields] |> Enum.into(MapSet.new())
    fields_b = idx_b[:fields] |> Enum.into(MapSet.new())
    where_fields_list = get_fields_list(constraints, [])
    where_fields = Enum.into(where_fields_list, MapSet.new())

    overlap_a = MapSet.intersection(where_fields, fields_a) |> MapSet.size()
    overlap_b = MapSet.intersection(where_fields, fields_b) |> MapSet.size()

    match_a? = overlap_a == MapSet.size(fields_a) and overlap_a == MapSet.size(where_fields)
    match_b? = overlap_b == MapSet.size(fields_b) and overlap_b == MapSet.size(where_fields)

    exact_match_short_circuit(match_a?, idx_a, match_b?, idx_b)

    # Most indexes will be Default indexes, so we can optimize for that case. Default index allows 0 or 1
    # Between ops, and it always must be the last field in the index.
    between_fields = for %QueryPlan.Between{field: field} <- constraints, do: field

    # The ordering of a query has the same index selection characteristics as a Between, as long
    # as there's not a Between already defined.
    {between_fields, montonicity_fields_list} =
      if between_fields == [] do
        ordering_fields =
          case ordering do
            [%QueryPlan.Order{field: field}] -> [field]
            _ -> []
          end

        {ordering_fields, where_fields_list ++ ordering_fields}
      else
        {between_fields, where_fields_list}
      end

    final_between_a? =
      length(between_fields) <= 1 and
        Enum.slice(idx_a[:fields], 0, length(montonicity_fields_list)) ==
          montonicity_fields_list

    final_between_b? =
      length(between_fields) <= 1 and
        Enum.slice(idx_b[:fields], 0, length(montonicity_fields_list)) ==
          montonicity_fields_list

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
      transactional_(tenant, tx, cache, source, fun)
    end)
  end

  def transactional_(tenant, tx, nil, source, fun) do
    {_vsn, metadata, validator} = get_metadata(tenant, tx, source, nil)

    apply_optimistically(tx, metadata, fun, validator, nil, nil)
  end

  def transactional_(tenant, tx, cache, source, fun) do
    now = System.monotonic_time(:millisecond)

    cache_key = Cache.key(tenant, source)
    cache_item = Cache.lookup(cache, cache_key)

    {mdv, metadata, validator} = get_metadata(tenant, tx, source, cache_item)

    Cache.update(cache, Cache.CacheItem.new(cache_key, mdv, metadata, now))

    apply_optimistically(tx, metadata, fun, validator, cache, cache_key)
  end

  defp apply_optimistically(tx, metadata, fun, validator, cache, cache_key) do
    fun.(tx, metadata)
  after
    unless validator.() do
      if cache, do: :ets.delete(cache, cache_key)
      :erlang.error(@metadata_operation_failed)
    end
  end

  defp get_metadata(tenant, tx, source, cache_item) do
    valid = fn -> true end

    case Map.get(@builtin_metadata, source, nil) do
      nil ->
        global_only_mdv = MetadataVersion.tx_get_new(tx) |> Future.result()

        # We skip the global check if this is not our first time through the transaction.
        # Otherwise, another tenant's index creation can cause us to retry more than is required.
        if not Tx.repeated?(tx) and Cache.CacheItem.match_global?(cache_item, global_only_mdv) do
          # Waiting on the global metadataVersion key is cheap (sent with every GRV). This buys
          # us the privilege of having a trivial validator. If the global metadataVersion matches,
          # the transaction will always succeed (wrt our Metadata checks)
          mdv = Cache.CacheItem.get_metadata_version(cache_item)
          metadata = Cache.CacheItem.get_metadata(cache_item)
          {mdv, metadata, valid}
        else
          mdv_future =
            MetadataVersion.tx_with_app(
              global_only_mdv,
              tenant,
              tx,
              @app_metadata_version_idx
            )

          claim_future = MigrationsPJ.tx_get_claim_status(tenant, tx, source)
          _get_metadata(tenant, tx, source, cache_item, mdv_future, claim_future)
        end

      metadata ->
        {nil, struct(__MODULE__, metadata), valid}
    end
  end

  defp _get_metadata(tenant, tx, source, nil, mdv_future, claim_future) do
    read_metadata(tenant, tx, source, mdv_future, claim_future)
  end

  defp _get_metadata(_tenant, _tx, _source, cache_item, mdv_future, claim_future) do
    get_metadata_from_cache(cache_item, mdv_future, claim_future)
  end

  defp get_metadata_from_cache(cache_item, mdv_future, claim_future) do
    mdv = Cache.CacheItem.get_metadata_version(cache_item)

    vsn_validator = fn ->
      [mdv2, claim_status] =
        [mdv_future, claim_future]
        |> Future.await_stream()
        |> Enum.map(&Future.result/1)

      {claim_active?, _} = claim_status

      Cache.CacheItem.match_local?(cache_item, mdv2) and not claim_active?
    end

    {mdv, Cache.CacheItem.get_metadata(cache_item), vsn_validator}
  end

  defp read_metadata(tenant, tx, source, mdv_future, claim_future) do
    metadata_future = tx_get_metadata(tenant, tx, source)

    [mdv, claim_status, metadata] =
      [mdv_future, claim_future, metadata_future]
      |> Future.await_stream()
      |> Enum.map(&Future.result/1)

    case claim_status do
      {claim_active?, []} ->
        {mdv, metadata, fn -> not claim_active? end}

      {_, partial_idxs} ->
        # Adding a write conflict here allows writes to commit at a lower latency
        # at the expense of migration taking longer, because we're not letting the
        # migration job starve us out
        MigrationsPJ.tx_add_claim_write_conflict(tenant, tx, source)
        metadata = %{metadata | partial_indexes: partial_idxs}
        {mdv, metadata, fn -> true end}
    end
  end

  defp metadata_range(tenant, source) do
    Pack.namespaced_range(tenant, source(), source, [])
  end

  def tx_get_metadata(tenant, tx, source) do
    {start_key, end_key} = metadata_range(tenant, source)

    Future.new(
      :tx_fold_future,
      {tx, :erlfdb.get_range(tx, start_key, end_key, wait: false)},
      &decode_metadata/1
    )
  end

  defp decode_metadata(result) do
    result
    |> Enum.map(fn {_key, fdb_value} -> Pack.from_fdb_value(fdb_value) end)
    # high priority first
    |> Enum.sort(&(Keyword.get(&1, :priority, 0) > Keyword.get(&2, :priority, 0)))
    |> new()
  end

  defp get_fields_list([], acc), do: List.flatten(Enum.reverse(acc))
  defp get_fields_list([%{field: field} | tail], acc), do: get_fields_list(tail, [field | acc])
  defp get_fields_list([%{fields: fields} | tail], acc), do: get_fields_list(tail, [fields | acc])
end
