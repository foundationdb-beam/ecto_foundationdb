defmodule EctoFoundationDB.Migration.SchemaMigration do
  # Defines a schema that works with a table that tracks schema migrations.
  @moduledoc false
  alias EctoFoundationDB.Migration.SchemaMigration
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

  def versions() do
    from(m in SchemaMigration, select: m.version)
  end

  def up(repo, version, opts \\ []) do
    %__MODULE__{version: version}
    |> repo.insert(opts)
  end

  def down(repo, version, opts) do
    from(m in SchemaMigration, where: m.version == type(^version, :integer))
    |> repo.delete_all(opts)
  end
end
