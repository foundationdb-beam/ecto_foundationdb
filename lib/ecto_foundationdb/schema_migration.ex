defmodule EctoFoundationDB.Migration.SchemaMigration do
  # Defines a schema that works with a table that tracks schema migrations.
  @moduledoc false
  alias EctoFoundationDB.Migration.SchemaMigration
  use Ecto.Schema

  import Ecto.Query, only: [from: 2]

  @schema_migrations_source "schema_migrations"

  def source(), do: @schema_migrations_source

  @primary_key false
  schema @schema_migrations_source do
    field(:version, :integer, primary_key: true)
    timestamps(updated_at: false)
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
