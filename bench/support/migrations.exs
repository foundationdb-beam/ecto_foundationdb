defmodule Ecto.Bench.CreateUser do
  alias Ecto.Bench.User
  use EctoFoundationDB.Migration

  def change do
    [
      create(index(User, [:name])),
      create(index(User, [:uuid]))
    ]
  end
end
