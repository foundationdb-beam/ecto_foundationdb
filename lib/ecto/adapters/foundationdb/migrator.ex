defmodule Ecto.Adapters.FoundationDB.Migrator do
  @moduledoc """
  Ecto FoundationDB is configured by default to manage database migrations
  triggered by actions taken on Tenants (See `Tenant.open/2`). This module contains
  the operations to manage those migrations.
  """

  require Logger

  @callback migrations() :: [{non_neg_integer(), module()}]

  alias Ecto.Adapters.FoundationDB
  alias Ecto.Adapters.FoundationDB.Options
  alias Ecto.Adapters.FoundationDB.Tenant
  alias Ecto.Adapters.FoundationDB.MigrationsPJ

  # todo
  @limit 1000

  @spec up_all(Ecto.Repo.t(), Options.t()) :: :ok
  def up_all(repo, options \\ []) do
    options = Keyword.merge(repo.config(), options)
    db = FoundationDB.db(repo)
    ids = Tenant.list(db, options)

    up_fun = fn id ->
      tenant = Tenant.db_open(db, id, options)
      up(repo, tenant, options)
    end

    max_concurrency = System.schedulers_online() * 2

    stream =
      Task.async_stream(ids, up_fun,
        ordered: false,
        max_concurrency: max_concurrency,
        timeout: :infinity
      )

    Stream.run(stream)
  end

  @spec up(Ecto.Repo.t(), Tenant.t() | Tenant.id(), Options.t()) :: :ok
  def up(repo, tenant_id, options) when is_binary(tenant_id) do
    db = FoundationDB.db(repo)
    tenant = Tenant.db_open(db, tenant_id, options)
    up(repo, tenant, options)
  end

  def up(repo, tenant, options) do
    migrator = Options.get(options, :migrator)

    if is_nil(migrator) do
      :ok
    else
      MigrationsPJ.transactional(repo, tenant, migrator, @limit)
    end

    :ok
  end
end
