defmodule Ecto.Adapters.FoundationDB.Migrator do
  @moduledoc """
  Ecto FoundationDB is configured by default to manage database migrations
  triggered by actions taken on Tenants (See `Tenant.open/2`). This module contains
  the operations to manage those migrations.
  """

  @callback options(repo :: Ecto.Repo.t()) :: Keyword.t()
  @callback migrations(repo :: Ecto.Repo.t()) :: [{non_neg_integer(), module()}]

  alias Ecto.Adapters.FoundationDB
  alias Ecto.Adapters.FoundationDB.Options
  alias Ecto.Adapters.FoundationDB.Tenant

  def up_all(repo) do
    options = repo.config()
    db = FoundationDB.db(repo)
    ids = Tenant.list(db, options)

    # Maybe too extreme?
    tasks =
      for id <- ids do
        Task.async(fn ->
          tenant = Tenant.open(db, id, options)
          up(repo, tenant)
        end)
      end

    Task.await_many(tasks)
  end

  def up(repo, tenant) do
    options = repo.config()
    migrator = Options.get(options, :migrator)

    if is_nil(migrator) do
      :ok
    else
      migrations = Kernel.apply(migrator, :migrations, [repo])

      up_options = Kernel.apply(migrator, :options, [repo])

      for {version, module} <- migrations do
        Ecto.Migrator.up(repo, version, module, up_options ++ [prefix: Tenant.to_prefix(tenant)])
      end
    end
  end
end
