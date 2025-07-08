defmodule EctoFoundationDB.Schemas.User2 do
  @moduledoc false
  # User2 is a copy of User, but only used for the large_migration_test.exs so
  # we can precisely control the indexes during migration

  use Ecto.Schema

  import Ecto.Changeset

  @primary_key {:id, :binary_id, autogenerate: true}

  schema "users2" do
    field(:name, :string)
    field(:notes, :string)

    timestamps()
  end

  def changeset(struct, attrs) do
    struct
    |> cast(attrs, [:name, :notes])
    |> validate_required([:name])
  end
end
