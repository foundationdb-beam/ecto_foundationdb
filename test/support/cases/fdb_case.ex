defmodule FDB.Case do
  @moduledoc false
  use ExUnit.CaseTemplate

  def init() do
    db()
    :ok
  end

  defp db() do
    case :persistent_term.get({__MODULE__, :db}, nil) do
      nil ->
        db = :erlfdb_sandbox.open("FDB.Case")
        :persistent_term.put({__MODULE__, :db}, db)
        db

      db ->
        db
    end
  end

  defp dir(db, name) do
    root = :erlfdb_directory.root(node_prefix: <<"FDB.Case">>, content_prefix: <<>>)
    :erlfdb_directory.create_or_open(db, root, name)
  end

  setup do
    db = db()
    dir = dir(db, "#{:erlang.unique_integer()}")

    on_exit(fn ->
      :ok = :erlfdb_directory.remove_if_exists(db, dir)
      nil
    end)

    [db: db, dir: dir]
  end
end
