defmodule EctoFoundationDB.Integration.Migration.UserIndex do
  @moduledoc false
  use Ecto.Adapters.FoundationDB.Migration

  def change() do
    [create(index(:users, [:name]))]
  end
end

defmodule EctoFoundationDB.Integration.Migration.EventIndex do
  @moduledoc false
  use Ecto.Adapters.FoundationDB.Migration

  def change() do
    [create(index(:events, [:timestamp], options: [indexer: :timeseries]))]
  end
end
