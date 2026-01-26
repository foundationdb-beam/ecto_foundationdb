defmodule FDBTest do
  use FDB.Case, async: true

  test "stream_range/4", %{db: db, dir: dir} do
    :erlfdb.transactional(db, fn tx ->
      for {k, v} <- [{{"range", 1}, "a"}, {{"range", 2}, "b"}, {{"range", 3}, "c"}],
          do: :erlfdb.set(tx, :erlfdb_directory.pack(dir, k), v)
    end)

    {start_key, end_key} = :erlfdb_directory.range(dir, {"range"})

    assert [{_, "a"}, {_, "b"}, {_, "c"}] = :erlfdb.get_range(db, start_key, end_key)

    assert [{_, "a"}, {_, "b"}] =
             :erlfdb.transactional(db, fn tx ->
               FDB.stream_range(tx, start_key, end_key, target_bytes: 1)
               |> Stream.take(2)
               |> Enum.to_list()
             end)

    assert [{_, "c"}, {_, "b"}] =
             :erlfdb.transactional(db, fn tx ->
               FDB.stream_range(tx, start_key, end_key, target_bytes: 1, reverse: true)
               |> Stream.take(2)
               |> Enum.to_list()
             end)
  end
end
