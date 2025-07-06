defmodule Ecto.Integration.MigrationsCase do
  @moduledoc false
  use ExUnit.CaseTemplate
  alias Ecto.Integration.TestRepo

  setup do
    context = TenantForCase.setup(TestRepo, migrator: nil, log: false)

    on_exit(fn ->
      TenantForCase.exit(TestRepo, context[:tenant_id])
    end)

    {:ok, context}
  end
end
