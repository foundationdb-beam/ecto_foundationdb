defmodule EctoFoundationDB.Integration.TestMigrator do
  @moduledoc false

  @behaviour Ecto.Adapters.FoundationDB.Migrator

  @impl true
  def options() do
    [log: false]
  end

  @impl true
  def migrations() do
    [
      {0, EctoFoundationDB.Integration.Migration.UserIndex},
      {1, EctoFoundationDB.Integration.Migration.EventIndex}
    ]
  end
end

defmodule EctoFoundationDB.Integration.TestMigrator2 do
  @moduledoc false

  @behaviour Ecto.Adapters.FoundationDB.Migrator

  @impl true
  def options() do
    [log: false]
  end

  @impl true
  def migrations() do
    [
      {0, EctoFoundationDB.Integration.Migration.UserIndex},
      {1, EctoFoundationDB.Integration.Migration.EventIndex},
      {2, EctoFoundationDB.Integration.Migration.IndexFromEventIndex}
    ]
  end
end
