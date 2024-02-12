defmodule Ecto.Adapters.FoundationDB.Transaction do
  alias Ecto.Adapters.FoundationDB.Layer.Tx

  def commit(db_or_tenant, fun) do
    fun = fn ->
      try do
        Tx.commit_proc(db_or_tenant, fun)
      rescue
        e ->
          {:exception, __MODULE__, e, __STACKTRACE__}
      end
    end

    res =
      fun
      |> Task.async()
      |> Task.await()

    case res do
      {:exception, __MODULE__, e, st} ->
        reraise e, st

      _ ->
        res
    end
  end
end
