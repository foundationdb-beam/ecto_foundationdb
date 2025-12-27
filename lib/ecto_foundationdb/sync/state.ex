defmodule EctoFoundationDB.Sync.State do
  @moduledoc false
  @data_key :ecto_fdb_sync_data

  alias EctoFoundationDB.Future
  alias EctoFoundationDB.WatchJanitor

  def data_key(), do: @data_key

  def get_futures(state, repo) do
    case get_in(state, [Access.key(:private), data_key(), repo, :futures]) do
      nil ->
        %{}

      futures ->
        futures
    end
  end

  def put_futures(state, repo, futures) do
    state
    |> update_in([Access.key(:private)], &default_map/1)
    |> update_in([Access.key(:private), data_key()], &default_map/1)
    |> update_in([Access.key(:private), data_key(), repo], &default_map/1)
    |> put_in([Access.key(:private), data_key(), repo, :futures], futures)
  end

  def futures_empty?(state, repo) do
    futures = get_futures(state, repo)
    std_f = Map.get(futures, :std, %{})
    idlist_f = Map.get(futures, :idlist, %{})
    map_size(std_f) == 0 and map_size(idlist_f) == 0
  end

  def update_futures(state, repo, key, labeled_f) do
    WatchJanitor.register(WatchJanitor.get!(repo), self(), Map.values(labeled_f))

    futures = get_futures(state, repo)
    f = Map.get(futures, key, %{})

    futures_to_cancel = Map.intersect(labeled_f, f)
    Enum.each(futures_to_cancel, fn {_, f} -> Future.cancel(f) end)

    f = Map.merge(f, labeled_f)
    put_futures(state, repo, Map.put(futures, key, f))
  end

  def merge_futures(state, repo, key, old_f, new_f) do
    WatchJanitor.register(WatchJanitor.get!(repo), self(), Map.values(new_f))
    futures = get_futures(state, repo)
    f = Map.merge(old_f, new_f)
    put_futures(state, repo, Map.put(futures, key, f))
  end

  def cancel_futures(state, repo, key) do
    futures = get_futures(state, repo)
    futures_to_cancel = Map.get(futures, key, %{})
    Enum.each(futures_to_cancel, fn {_, f} -> Future.cancel(f) end)
    labels = to_external_labels(futures_to_cancel, key)
    {labels, put_futures(state, repo, Map.put(futures, key, %{}))}
  end

  def cancel_futures(state, repo, key, labels) do
    futures = get_futures(state, repo)
    {futures_to_cancel, rem} = Map.get(futures, key, %{}) |> Map.split(labels)
    Enum.each(futures_to_cancel, fn {_, f} -> Future.cancel(f) end)
    labels = to_external_labels(futures_to_cancel, key)
    {labels, put_futures(state, repo, Map.put(futures, key, rem))}
  end

  defp to_external_labels(futures_to_cancel, :std), do: Map.keys(futures_to_cancel)

  defp to_external_labels(futures_to_cancel, :idlist) do
    futures_to_cancel
    |> Map.keys()
    |> Enum.map(fn {l, _id} -> l end)
    |> :lists.usort()
  end

  def get_callbacks(state, repo) do
    case get_in(state, [Access.key(:private), data_key(), repo, Access.key(:callbacks)]) do
      nil ->
        %{}

      callbacks ->
        callbacks
    end
  end

  def put_callbacks(state, repo, callbacks) do
    state
    |> update_in([Access.key(:private)], &default_map/1)
    |> update_in([Access.key(:private), data_key()], &default_map/1)
    |> update_in([Access.key(:private), data_key(), repo], &default_map/1)
    |> put_in([Access.key(:private), data_key(), repo, Access.key(:callbacks)], callbacks)
  end

  def default_map(nil), do: %{}
  def default_map(map), do: map
end
