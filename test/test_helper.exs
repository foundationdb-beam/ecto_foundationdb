Logger.configure(level: :info)

Application.put_env(:ecto, :primary_key_type, :binary_id)
Application.put_env(:ecto, :async_integration_tests, false)

ecto = Mix.Project.deps_paths()[:ecto]
Code.require_file("#{ecto}/integration_test/support/schemas.exs", __DIR__)

alias Ecto.Integration.TestRepo

Application.put_env(:ecto_foundationdb, TestRepo, key_delimiter: "/")

defmodule Ecto.Integration.Case do
  use ExUnit.CaseTemplate

  alias Ecto.Adapters.FoundationDB.Sandbox

  setup do
    {tenant_name, tenant} = Sandbox.checkout(TestRepo)
    on_exit(fn -> Ecto.Adapters.FoundationDB.Sandbox.checkin(TestRepo) end)
    {:ok, tenant: tenant, tenant_name: tenant_name}
  end
end

{:ok, _} = TestRepo.start_link()

ExUnit.start()
