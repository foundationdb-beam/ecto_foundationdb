defmodule EctoFoundationDB.Schemas.Event do
  @moduledoc false

  use Ecto.Schema

  alias EctoFoundationDB.Versionstamp

  import Ecto.Changeset

  # Using write_primary: false means that the index will be the
  # only way to access the Event. There will be no entry existing
  # on only the primary key
  @schema_context write_primary: false

  @primary_key {:id, Versionstamp, autogenerate: false}

  schema "events" do
    field(:date, :date)
    field(:user_id, :string)
    field(:time, :time_usec)
    field(:data, :string)

    timestamps()
  end

  def changeset(struct, attrs) do
    struct
    |> cast(attrs, [:date, :user_id, :time])
    |> validate_required([:date, :user_id, :time])
  end
end
