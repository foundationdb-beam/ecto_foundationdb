defmodule Ecto.Bench.CreateUser do
  use Ecto.Adapters.FoundationDB.Migration

  def change do
    [
      create(index(:users, [:name])),
      create(index(:users, [:uuid]))
    ]
  end
end

defmodule Ecto.Bench.Migrator do
  @behaviour Ecto.Adapters.FoundationDB.Migrator

  def options(), do: []
  def migrations() do
    [{0, Ecto.Bench.CreateUser}]
  end
end
