defmodule Ecto.Bench.FDBRepo do
  use Ecto.Repo, otp_app: :ecto_foundationdb, adapter: Ecto.Adapters.FoundationDB, log: false
end
