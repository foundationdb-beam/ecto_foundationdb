defmodule EctoFoundationDB.Integration.TestMigrator do
  @moduledoc false

  @behaviour Ecto.Adapters.FoundationDB.Migrator

  @impl true
  def options(Ecto.Integration.TestRepo) do
    [log: false]
  end

  @impl true
  def migrations(Ecto.Integration.TestRepo) do
    [
      {0, EctoFoundationDB.Integration.Migration.UserIndex},
      {1, EctoFoundationDB.Integration.Migration.EventIndex}
    ]
  end
end
