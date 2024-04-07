defmodule Ecto.Integration.TestRepo do
  @moduledoc """
  This is the Ecto Repo module under which all integration tests run.

  The integration tests use `EctoFoundationDB.Sandbox' for managing
  a standalone FoundationDB cluster.
  """

  use Ecto.Repo, otp_app: :ecto_foundationdb, adapter: Ecto.Adapters.FoundationDB

  def uuid() do
    Ecto.UUID
  end
end
