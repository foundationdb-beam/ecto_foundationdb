defmodule Ecto.Bench.CreateUser do
  alias Ecto.Bench.User
  use Ecto.Adapters.FoundationDB.Migration

  def change do
    [
      create(index(User, [:name])),
      create(index(User, [:uuid]))
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
