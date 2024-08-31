defmodule EctoIntegrationTxnTest do
  use Ecto.Integration.Case, async: true

  alias Ecto.Integration.TestRepo
  alias EctoFoundationDB.Schemas.User

  test "foo", context do
    tenant = context[:tenant]

    TestRepo.transaction(
      fn ->
        users = [%{name: "John"}, %{name: "James"}]
        TestRepo.insert_all(User, users)
      end,
      prefix: tenant
    )
  end
end
