defmodule Ecto.Adapters.FoundationDB.Transaction do
  alias Ecto.Adapters.FoundationDB.Record.Tx

  def commit(db_or_tenant, fun) do
    fn -> Tx.commit_proc(db_or_tenant, fun) end
    |> Task.async()
    |> Task.await()
  end
end
