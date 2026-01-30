defmodule Ecto.Integration.TinyRepo do
  @moduledoc false
  use Ecto.Repo, otp_app: :ecto_foundationdb, adapter: Ecto.Adapters.FoundationDB

  use EctoFoundationDB.Migrator

  @impl true
  def migrations() do
    [
      {0, EctoFoundationDB.Integration.Migration.UserIndex}
    ]
  end
end
