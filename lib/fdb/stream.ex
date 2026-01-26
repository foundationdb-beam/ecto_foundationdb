defmodule FDB.Stream do
  @moduledoc false

  # Wraps the :erlfdb_range_iterator in an Elixir Stream. Perhaps part of
  # a new library someday.
  def range(tx, start_key, end_key, options \\ []) do
    start_fun = fn ->
      {:ok, iterator} = :erlfdb_range_iterator.start(tx, start_key, end_key, options)
      iterator
    end

    Stream.resource(start_fun, &next_/1, &after_/1)
  end

  def from_iterator(iterator) do
    Stream.resource(fn -> iterator end, &next_/1, &after_/1)
  end

  defp next_(iterator) do
    case :erlfdb_range_iterator.next(iterator) do
      {:done, state} ->
        {:halt, state}

      {:ok, result, state} ->
        {result, state}
    end
  end

  def after_(iterator), do: :erlfdb_range_iterator.stop(iterator)
end
