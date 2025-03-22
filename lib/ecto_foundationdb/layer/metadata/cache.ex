defmodule EctoFoundationDB.Layer.Metadata.Cache do
  @moduledoc false
  alias EctoFoundationDB.Layer.Metadata

  defmodule CacheItem do
    @moduledoc false
    alias EctoFoundationDB.Layer.MetadataVersion

    def new(key, mdv, metadata, ts) do
      {key, mdv, metadata, ts}
    end

    def get_metadata_version({_key, mdv, _metadata, _ts}), do: mdv
    def get_metadata({_key, _mdv, metadata, _ts}), do: metadata

    def match_global?(nil, _mdv_b), do: false

    def match_global?({_key, mdv_a, _metadata, _ts}, mdv_b),
      do: MetadataVersion.match_global?(mdv_a, mdv_b)

    def match_local?(nil, _), do: false

    def match_local?({_key, mdv_a, _metadata, _ts}, mdv_b),
      do: MetadataVersion.match_local?(mdv_a, mdv_b)
  end

  def key(tenant, source) do
    {tenant.id, source}
  end

  def lookup(nil, _key), do: nil

  def lookup(table, key) do
    case :ets.lookup(table, key) do
      [item] ->
        item

      _ ->
        nil
    end
  end

  def update(nil, _new), do: :ok
  def update(_table, nil), do: :ok

  def update(
        table,
        {key, _mdv, %Metadata{partial_indexes: partial_indexes}, _ts}
      )
      when length(partial_indexes) > 0 do
    delete(table, key)
    :ok
  end

  def update(table, cache_item) do
    :ets.insert(table, cache_item)

    :ok
  end

  def delete(table, key) do
    :ets.delete(table, key)
  end
end
