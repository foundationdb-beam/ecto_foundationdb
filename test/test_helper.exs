Logger.configure(level: :info)

Application.put_env(:ecto, :primary_key_type, :binary_id)
Application.put_env(:ecto, :async_integration_tests, false)

ecto = Mix.Project.deps_paths()[:ecto]
Code.require_file("#{ecto}/integration_test/support/schemas.exs", __DIR__)

alias Ecto.Integration.TestRepo

Application.put_env(:ecto_foundationdb, TestRepo,
  open_db: &Ecto.Adapters.FoundationDB.Sandbox.open_db/0,
  storage_id: Ecto.Adapters.FoundationDB.Sandbox
)

defmodule Ecto.Integration.Case do
  use ExUnit.CaseTemplate

  alias Ecto.Adapters.FoundationDB.Sandbox

  setup do
    tenant = Sandbox.checkout(TestRepo, "MyTenant")
    other_tenant = Sandbox.checkout(TestRepo, "OtherTenant")
    on_exit(fn ->
      Ecto.Adapters.FoundationDB.Sandbox.checkin(TestRepo, "MyTenant")
      Ecto.Adapters.FoundationDB.Sandbox.checkin(TestRepo, "OtherTenant")
    end)

    {:ok,
     tenant: tenant,
     tenant_id: "MyTenant",
     other_tenant: other_tenant,
     other_tenant_id: "OtherTenant"}
  end
end

_ = Ecto.Adapters.FoundationDB.storage_down(TestRepo.config())
:ok = Ecto.Adapters.FoundationDB.storage_up(TestRepo.config())

{:ok, _} = TestRepo.start_link()

#:ok = Ecto.Migrator.up(TestRepo, 0, EctoFoundationDB.Integration.Migration, prefix: "MyTenant", log: false)

ExUnit.start()
