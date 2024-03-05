defmodule Ecto.Adapters.FoundationDB.Transaction do
  @moduledoc """
  This module defines the API that allows an application to excute FDB Transactions
  that they define. Please be aware of the
  [limitations that FoundationDB](https://apple.github.io/foundationdb/developer-guide.html#transaction-basics)
  imposes on transactions.

  For example, a transaction must complete
  [within 5 seconds](https://apple.github.io/foundationdb/developer-guide.html#long-running-transactions).
  """
  alias Ecto.Adapters.FoundationDB.Database
  alias Ecto.Adapters.FoundationDB.Layer.Tx
  alias Ecto.Adapters.FoundationDB.Tenant

  @doc """
  Executes the given function in a transaction on the database.

  If you provide an arity-0 function, your function will be executed in
  a newly spawned process. This is to ensure that EctoFoundationDB can
  safely manage the process dictionary.
  """
  @spec commit(Database.t() | Tenant.t(), function()) :: any()
  def commit(db_or_tenant, fun) when is_function(fun, 0) do
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

  def commit(db_or_tenant, fun) when is_function(fun, 1) do
    :erlfdb.transactional(db_or_tenant, fun)
  end
end
