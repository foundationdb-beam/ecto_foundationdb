defmodule EctoFoundationDB.Layer.Metadata.Cache do
  @moduledoc false
  alias EctoFoundationDB.Layer.Metadata

  defmodule CacheItem do
    @moduledoc false
    def new(key, vsn, metadata, ts) do
      {key, {vsn, metadata}, ts}
    end

    def from_ets_entry({key, vsn_map, ts}) when is_map(vsn_map) do
      {vsn, metadata} = Enum.max(vsn_map)
      new(key, vsn, metadata, ts)
    end

    def get_version({_key, {vsn, _metadata}, _ts}), do: vsn
    def get_metadata({_key, {_vsn, metadata}, _ts}), do: metadata

    def merge_into_ets_entry({key, vsn_map, ts1}, {key, {vsn, metadata}, ts2})
        when is_map(vsn_map) do
      {key, Map.put(vsn_map, vsn, metadata), max(ts1, ts2)}
    end

    def new_ets_entry({key, {vsn, metadata}, ts}) do
      {key, %{vsn => metadata}, ts}
    end
  end

  def key(tenant, source) do
    {tenant.id, source}
  end

  def lookup(nil, _key), do: nil

  def lookup(table, key) do
    case :ets.lookup(table, key) do
      [entry] ->
        CacheItem.from_ets_entry(entry)

      _ ->
        nil
    end
  end

  def update(nil, _old, _new), do: :ok

  def update(
        table,
        {key, {vsn, _metadata1}, old_timestamp},
        {key, {vsn, _metadata2}, new_timestamp}
      ) do
    # Note: I think this is a bug, we can't guarantee that someone else didn't update the timestamp already.
    # It means that we could end up storing future timestamps. But it doesn't really matter, if the timestamp
    # is in the future then the cache is being hit, so we wouldn't want to expire anyway
    diff = new_timestamp - old_timestamp

    try do
      :ets.update_counter(table, key, {3, diff})
    catch
      _, _ ->
        # Someone else removed the cache entry
        :ok
    end

    :ok
  end

  def update(table, _old_citem, {key, {_vsn, %Metadata{partial_indexes: partial_indexes}}, _ts})
      when length(partial_indexes) > 0 do
    delete(table, key)
    :ok
  end

  def update(table, _old_citem, cache_item = {key, {_vsn, _metadata}, _ts}) do
    # Not transaction-aware, but only additive, so it should be safe.

    entry =
      case :ets.lookup(table, key) do
        [entry] ->
          CacheItem.merge_into_ets_entry(entry, cache_item)

        _ ->
          CacheItem.new_ets_entry(cache_item)
      end

    :ets.insert(table, entry)

    :ok
  end

  def delete(table, key) do
    :ets.delete(table, key)
  end
end
