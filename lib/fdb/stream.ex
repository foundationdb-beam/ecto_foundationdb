defmodule FDB.Stream do
  @moduledoc false

  # Wraps the :erlfdb_range_iterator in an Elixir Stream. Perhaps part of
  # a new library someday.
  @spec range(:erlfdb.transaction(), :erlfdb.key(), :erlfdb.key(), [:erlfdb.fold_option()]) ::
          Enumerable.t()
  def range(tx, start_key, end_key, options \\ []) do
    start_fun = fn ->
      iterator = :erlfdb_range_iterator.start(tx, start_key, end_key, options)
      {:cont, iterator}
    end

    Stream.resource(start_fun, &next_/1, &after_/1)
  end

  @spec from_iterator(:erlfdb_iterator.iterator()) :: Enumerable.t()
  def from_iterator(iterator) do
    Stream.resource(fn -> {:cont, iterator} end, &next_/1, &after_/1)
  end

  defp next_({:halt, iterator}), do: {:halt, {:halt, iterator}}

  defp next_({:cont, iterator}) do
    case :erlfdb_iterator.next(iterator) do
      {:halt, iterator} ->
        {:halt, {:halt, iterator}}

      {:halt, result, iterator} ->
        {result, {:halt, iterator}}

      {:cont, result, iterator} ->
        {result, {:cont, iterator}}
    end
  end

  def after_({_, iterator}) do
    :erlfdb_iterator.stop(iterator)
  end
end
