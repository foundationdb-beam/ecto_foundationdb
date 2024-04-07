defmodule Ecto.Adapters.FoundationDB.EctoAdapterTransaction do
  @moduledoc false
  alias Ecto.Adapters.FoundationDB
  alias EctoFoundationDB.Layer.Tx
  @behaviour Ecto.Adapter.Transaction

  @rollback :__ectofdbtxrollback__

  @doc """
  Runs the given function inside a transaction.

  Returns `{:ok, value}` if the transaction was successful where `value`
  is the value return by the function or `{:error, value}` if the transaction
  was rolled back where `value` is the value given to `rollback/1`.
  """
  @impl true
  def transaction(_adapter_meta, options, function) when is_function(function, 0) do
    FoundationDB.transactional(options[:prefix], fn ->
      function.()
    end)
  catch
    {@rollback, value} -> {:error, value}
  end

  def transaction(_adapter_meta, options, function) when is_function(function, 1) do
    FoundationDB.transactional(options[:prefix], fn repo ->
      function.(repo)
    end)
  catch
    {:__ectofdbtxrollback__, value} -> {:error, value}
  end

  @doc """
  Returns true if the given process is inside a transaction.
  """
  @impl true
  def in_transaction?(_adapter_meta) do
    Tx.in_tx?()
  end

  @doc """
  Rolls back the current transaction.

  The transaction will return the value given as `{:error, value}`.

  See `c:Ecto.Repo.rollback/1`.
  """
  @impl true
  def rollback(adapter_meta, value) do
    if in_transaction?(adapter_meta) do
      throw({@rollback, value})
    end
  end
end
