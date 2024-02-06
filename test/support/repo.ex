defmodule Ecto.Integration.TestRepo do
  @moduledoc false

  use Ecto.Repo, otp_app: :ecto_foundationdb, adapter: Ecto.Adapters.FoundationDB

  def uuid() do
    Ecto.UUID
  end
end
