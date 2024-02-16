defmodule EctoFoundationDB.Schemas.Event do
  @moduledoc false

  use Ecto.Schema

  import Ecto.Changeset

  @schema_context usetenant: true, write_primary: false

  @primary_key {:id, :binary_id, autogenerate: true}

  schema "events" do
    field(:timestamp, :naive_datetime_usec)

    timestamps()
  end

  def changeset(struct, attrs) do
    struct
    |> cast(attrs, [:timestamp])
    |> validate_required([:timestamp])
  end
end
