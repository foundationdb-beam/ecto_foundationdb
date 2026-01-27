defmodule FDB.Stream do
  @moduledoc false

  # Wraps the :erlfdb_range_iterator in an Elixir Stream. Perhaps part of
  # a new library someday.
  @spec range(:erlfdb.transaction(), :erlfdb.key(), :erlfdb.key(), [:erlfdb.fold_option()]) ::
          Enumerable.t()
  def range(tx, start_key, end_key, options \\ []) do
    start_fun = fn ->
      {:ok, iterator} = :erlfdb_range_iterator.start(tx, start_key, end_key, options)
      iterator
    end

    Stream.resource(start_fun, &next_/1, &after_/1)
  end

  @spec from_iterator(:erlfdb_iterator.iterator()) :: Enumerable.t()
  def from_iterator(iterator) do
    Stream.resource(fn -> iterator end, &next_/1, &after_/1)
  end

  defp next_(iterator) do
    case :erlfdb_iterator.next(iterator) do
      {:done, iterator} ->
        {:halt, iterator}

      {:ok, result, iterator} ->
        {result, iterator}
    end
  end

  def after_(iterator), do: :erlfdb_iterator.stop(iterator)
end
