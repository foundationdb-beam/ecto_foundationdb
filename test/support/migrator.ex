defmodule EctoFoundationDB.Integration.TestMigrator do
  @moduledoc false

  @behaviour Ecto.Adapters.FoundationDB.Migrator

  @impl true
  def migrations() do
    [
      {0, EctoFoundationDB.Integration.Migration.UserIndex},
      {1, EctoFoundationDB.Integration.Migration.EventIndex}
    ]
  end
end
