defmodule EctoFoundationDB.Schemas.User do
  @moduledoc false

  use Ecto.Schema

  import Ecto.Changeset

  @schema_context usetenant: true

  @primary_key {:id, :binary_id, autogenerate: true}

  schema "users" do
    field(:name, :string)

    timestamps()
  end

  def changeset(struct, attrs) do
    struct
    |> cast(attrs, [:name])
    |> validate_required([:name])
  end
end
