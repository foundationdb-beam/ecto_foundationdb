defmodule EctoFoundationDB.Schemas.Global do
  @moduledoc false

  use Ecto.Schema

  import Ecto.Changeset

  # 'Global' schema exists to test paths where usetenant is false
  # @schema_context usetenant: false

  @primary_key {:id, :binary_id, autogenerate: true}

  schema "globals" do
    field(:name, :string)

    timestamps()
  end

  def changeset(struct, attrs) do
    struct
    |> cast(attrs, [:name])
    |> validate_required([:name])
  end
end
