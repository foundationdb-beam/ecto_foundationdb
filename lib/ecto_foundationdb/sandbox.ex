defmodule EctoFoundationDB.Sandbox do
  @moduledoc """
  A module for managing a sandbox FoundationDB cluster. This allows a developer to create
  a space under which it should be safe to write tests.

  When using this module, it creates a directory in the project root called `.erlfdb`. It
  is safe to delete this directory when you no longer need it (e.g. after test execution)
  """
  alias EctoFoundationDB.Database
  alias EctoFoundationDB.Options
  alias EctoFoundationDB.Tenant

  @spec open_db() :: Database.t()
  def open_db() do
    get_or_create_test_db()
  end

  @spec checkout(Ecto.Repo.t(), Tenant.id(), Options.t()) :: Tenant.t()
  def checkout(repo, id, options \\ []) when is_binary(id) do
    Tenant.open_empty!(repo, id, options)
  end

  @spec checkin(Ecto.Repo.t(), Tenant.id()) :: :ok
  def checkin(repo, id) when is_binary(id) do
    Tenant.clear_delete!(repo, id)
    :ok
  end

  defp get_or_create_test_db() do
    case :persistent_term.get({__MODULE__, :database}, nil) do
      nil ->
        new_db = :erlfdb_util.get_test_db([])
        :persistent_term.put({__MODULE__, :database}, new_db)
        new_db

      already_initted_db ->
        already_initted_db
    end
  end
end
