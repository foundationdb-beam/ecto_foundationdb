defmodule Ecto.Integration.TestManagedTenantRepo do
  @moduledoc false

  use Ecto.Repo, otp_app: :ecto_foundationdb, adapter: Ecto.Adapters.FoundationDB
end
