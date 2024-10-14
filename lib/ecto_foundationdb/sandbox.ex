defmodule EctoFoundationDB.Sandbox do
  @moduledoc """
  A module for managing a sandbox FoundationDB cluster. This allows a developer to create
  a space under which it should be safe to write tests.

  When using this module, it creates a directory in the project root called `.erlfdb`. It
  is safe to delete this directory when you no longer need it (e.g. after test execution)

  See [Testing with EctoFoundationDB](testing.html) for more.
  """
  alias EctoFoundationDB.Database
  alias EctoFoundationDB.Options
  alias EctoFoundationDB.Sandbox.Sandboxer
  alias EctoFoundationDB.Tenant

  @spec open_db(Ecto.Repo.t()) :: Database.t()
  def open_db(repo) do
    [{Ecto.Adapters.FoundationDB.Supervisor, sup, :supervisor, _}] =
      Supervisor.which_children(repo)

    repo_children = Supervisor.which_children(sup)
    {Sandboxer, pid, :worker, _} = List.keyfind!(repo_children, Sandboxer, 0)

    Sandboxer.get_or_create_test_db(pid)
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
end
