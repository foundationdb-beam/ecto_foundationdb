defmodule Ecto.Adapters.FoundationDB.Migration.SchemaMigration do
  # Defines a schema that works with a table that tracks schema migrations.
  @moduledoc false
  alias Ecto.Adapters.FoundationDB.Migration.SchemaMigration
  use Ecto.Schema

  @schema_context usetenant: true

  import Ecto.Query, only: [from: 2]

  # We put the schema_migrations source at the end of the keyspace so that
  # we can empty out a tenant's data without changing its migrations
  @schema_migrations_source "\xFFschema_migrations"

  def source(), do: @schema_migrations_source

  @primary_key false
  schema @schema_migrations_source do
    field(:version, :integer, primary_key: true)
    timestamps(updated_at: false)
  end

  # The migration flag is used to signal to the repository
  # we are in a migration operation.
  @default_opts [
    timeout: :infinity,
    log: false,
    schema_migration: true,
    telemetry_options: [schema_migration: true]
  ]

  def versions(repo, config, prefix) do
    {repo, _source} = get_repo_and_source(repo, config)
    from_opts = [prefix: prefix] ++ @default_opts

    query =
      if Keyword.get(config, :migration_cast_version_column, false) do
        from(m in SchemaMigration, select: type(m.version, :integer))
      else
        from(m in SchemaMigration, select: m.version)
      end

    {repo, query, from_opts}
  end

  def up(repo, config, version, opts) do
    {repo, _source} = get_repo_and_source(repo, config)

    %__MODULE__{version: version}
    |> repo.insert(default_opts(opts))
  end

  def down(repo, config, version, opts) do
    {repo, _source} = get_repo_and_source(repo, config)

    from(m in SchemaMigration, where: m.version == type(^version, :integer))
    |> repo.delete_all(default_opts(opts))
  end

  def get_repo_and_source(repo, config) do
    {Keyword.get(config, :migration_repo, repo), @schema_migrations_source}
  end

  defp default_opts(opts) do
    Keyword.merge(
      @default_opts,
      prefix: opts[:prefix],
      log: Keyword.get(opts, :log_adapter, false)
    )
  end
end
