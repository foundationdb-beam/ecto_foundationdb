defmodule EctoFoundationDB.Sync.State do
  @moduledoc false
  @data_key :ecto_fdb_sync_data

  alias EctoFoundationDB.Future
  alias EctoFoundationDB.WatchJanitor

  def data_key(), do: @data_key

  def get_futures(state, repo) do
    case get_in(state, [Access.key(:private), data_key(), repo, Access.key(:futures)]) do
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
    |> put_in([Access.key(:private), data_key(), repo, Access.key(:futures)], futures)
  end

  def merge_futures(state, repo, new_futures) do
    WatchJanitor.register(WatchJanitor.get!(repo), self(), Map.values(new_futures))
    futures = get_futures(state, repo)
    futures_to_cancel = Map.intersect(new_futures, futures)
    Enum.each(futures_to_cancel, fn {_, f} -> Future.cancel(f) end)
    put_futures(state, repo, Map.merge(futures, new_futures))
  end

  def cancel_futures(state, repo) do
    futures_to_cancel = get_futures(state, repo)
    Enum.each(futures_to_cancel, fn {_, f} -> Future.cancel(f) end)
    put_futures(state, repo, %{})
  end

  def cancel_futures(state, repo, labels) do
    futures = get_futures(state, repo)
    {futures_to_cancel, futures_to_keep} = Map.split(futures, labels)
    Enum.each(futures_to_cancel, fn {_, f} -> Future.cancel(f) end)
    put_futures(state, repo, futures_to_keep)
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
