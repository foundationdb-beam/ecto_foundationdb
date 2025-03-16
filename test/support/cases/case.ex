defmodule Ecto.Integration.Case do
  @moduledoc false
  use ExUnit.CaseTemplate
  alias Ecto.Integration.TestRepo

  setup do
    context = TenantsForCase.setup(TestRepo, log: false)

    on_exit(fn ->
      TenantsForCase.exit(TestRepo, context[:tenant_id], context[:other_tenant_id])
    end)

    {:ok, context}
  end
end
