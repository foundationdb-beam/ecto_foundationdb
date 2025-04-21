defmodule EctoFoundationDB.Schemas.QueueItem do
  @moduledoc false

  use Ecto.Schema

  alias EctoFoundationDB.Versionstamp

  @primary_key {:id, Versionstamp, autogenerate: false}

  schema "queue" do
    field(:author, :string)
    field(:data, :binary)
  end
end
