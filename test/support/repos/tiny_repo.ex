defmodule Ecto.Integration.TinyRepo do
  @moduledoc false
  use Ecto.Repo, otp_app: :ecto_foundationdb, adapter: Ecto.Adapters.FoundationDB

  use EctoFoundationDB.Migrator

  defmodule TinyMigration do
    @moduledoc false
    alias EctoFoundationDB.Schemas.User
    use EctoFoundationDB.Migration

    @impl true
    def change() do
      [create(index(User, [:name]))]
    end
  end

  @impl true
  def migrations() do
    [
      {0, TinyMigration}
    ]
  end
end
