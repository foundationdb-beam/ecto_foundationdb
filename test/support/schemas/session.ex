defmodule EctoFoundationDB.Schemas.Session do
  @moduledoc false

  use Ecto.Schema

  alias EctoFoundationDB.Versionstamp

  @primary_key {:id, Versionstamp, partition_by: :user_id, autogenerate: false}

  schema "sessions" do
    field(:user_id, :string)
    field(:data, :string)
  end
end
