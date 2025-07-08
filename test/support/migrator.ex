defmodule EctoFoundationDB.Integration.TestMigrator do
  @moduledoc false

  use EctoFoundationDB.Migrator

  @impl true
  def migrations() do
    [
      {0, EctoFoundationDB.Integration.Migration.UserIndex},
      {1, EctoFoundationDB.Integration.Migration.UserSchemaMetadata},
      {2, EctoFoundationDB.Integration.Migration.EventIndex},
      {3, EctoFoundationDB.Integration.Migration.QueueItemIndex}
    ]
  end
end
