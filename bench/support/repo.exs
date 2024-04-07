defmodule Ecto.Bench.FDBRepo do
  use Ecto.Repo, otp_app: :ecto_foundationdb, adapter: Ecto.Adapters.FoundationDB, log: false

  use EctoFoundationDB.Migrator

  def migrations() do
    [{0, Ecto.Bench.CreateUser}]
  end
end
