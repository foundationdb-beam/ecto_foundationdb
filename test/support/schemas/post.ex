defmodule EctoFoundationDB.Schemas.Post do
  @moduledoc false

  use Ecto.Schema

  alias EctoFoundationDB.Schemas.User

  import Ecto.Changeset

  @primary_key {:id, :binary_id, autogenerate: true}

  schema "posts" do
    field(:title, :string)
    field(:content, :string)
    belongs_to(:user, User, type: :binary_id)

    timestamps()
  end

  def changeset(struct, attrs) do
    struct
    |> cast(attrs, [:title, :content])
  end
end
