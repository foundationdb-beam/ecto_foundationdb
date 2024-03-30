defmodule Ecto.Adapters.FoundationDB.Migrator do
  @moduledoc """
  Ecto FoundationDB is configured by default to manage database migrations
  triggered by actions taken on Tenants (See `Tenant.open/2`). This module contains
  the operations to manage those migrations.
  """

  require Logger

  @callback options() :: Keyword.t()
  @callback migrations() :: [{non_neg_integer(), module()}]

  alias Ecto.Adapters.FoundationDB
  alias Ecto.Adapters.FoundationDB.Migration.Runner
  alias Ecto.Adapters.FoundationDB.Migration.SchemaMigration
  alias Ecto.Adapters.FoundationDB.Options
  alias Ecto.Adapters.FoundationDB.Tenant

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
        max_concurrency: max_concurrency
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
      true = Code.ensure_loaded?(migrator)

      case apply_on_migrator(migrator, :migrations, 0, [], []) do
        [] ->
          :ok

        migrations ->
          up(repo, tenant, migrator, migrations, options)
      end
    end
  end

  def up(repo, tenant, migrator, migrations, _options) do
    {last_version, _module} = List.last(migrations)

    if already_up?(tenant, repo, last_version) do
      :ok
    else
      up_options = apply_on_migrator(migrator, :options, 0, [], [])

      for {version, module} <- migrations do
        mod_up(tenant, repo, version, module, up_options ++ [prefix: tenant])
      end

      :ok
    end
  end

  def apply_on_migrator(nil, _function, _arity, _args, default), do: default

  def apply_on_migrator(module, function, arity, args, default) do
    if Code.ensure_loaded?(module) and function_exported?(module, function, arity) do
      apply(module, function, args)
    else
      default
    end
  end

  def already_up?(tenant, repo, version) do
    config = repo.config()

    {migration_repo, query, all_opts} = SchemaMigration.versions(repo, config, tenant)

    versions = migration_repo.all(query, all_opts)
    version in versions
  end

  # Copied from Ecto.Migrator.up/4
  def mod_up(tenant, repo, version, module, opts \\ []) do
    config = repo.config()

    opts =
      opts
      |> Keyword.put(:log, migrator_log(opts))

    repo.transaction(fn -> tx_do_up(repo, config, version, module, opts) end, prefix: tenant)
  end

  def tx_do_up(repo, config, version, module, opts) do
    {migration_repo, query, all_opts} = SchemaMigration.versions(repo, config, opts[:prefix])
    versions = migration_repo.all(query, all_opts)

    if version in versions do
      :already_up
    else
      result = do_up(repo, config, version, module, opts)

      if version != Enum.max([version | versions]) do
        latest = Enum.max(versions)

        message = """
        You are running migration #{version} but an older \
        migration with version #{latest} has already run.

        This can be an issue if you have already ran #{latest} in production \
        because a new deployment may migrate #{version} but a rollback command \
        would revert #{latest} instead of #{version}.

        If this can be an issue, we recommend to rollback #{version} and change \
        it to a version later than #{latest}.
        """

        warning_or_raise(opts[:string_version_order], message)
      end

      result
    end
  end

  defp warning_or_raise(true, message), do: raise(Ecto.MigrationError, message)
  defp warning_or_raise(_, message), do: Logger.warning(message)

  # Copied from Ecto.Migrator.up/4
  defp migrator_log(opts) do
    Keyword.get(opts, :log_adapter, false)
  end

  # Copied from Ecto.Migrator.up/4
  defp do_up(repo, config, version, module, opts) do
    result =
      attempt(repo, config, version, module, :forward, :up, :up, opts) ||
        attempt(repo, config, version, module, :forward, :change, :up, opts) ||
        {:error,
         Ecto.MigrationError.exception(
           "#{inspect(module)} does not implement a `up/0` or `change/0` function"
         )}

    {:ok, _} = SchemaMigration.up(repo, config, version, opts)

    result
  end

  # Copied from Ecto.Migrator.up/4
  defp attempt(repo, config, version, module, direction, operation, reference, opts) do
    if Code.ensure_loaded?(module) and function_exported?(module, operation, 0) do
      Runner.run(repo, config, version, module, direction, operation, reference, opts)
      :ok
    end
  end
end
