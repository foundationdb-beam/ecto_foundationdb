# Testing with EctoFoundationDB

Setting up your app to use a FoundationDB Sandbox is very easy!

First, in `config/test.exs`:

```elixir
config :my_app, MyApp.Repo,
  open_db: &EctoFoundationDB.Sandbox.open_db/0
```

Next, set up an ExUnit Case that will provide you with a new Tenant to use
in your tests. This step is optional, but recommended: it will make writing
each test frictionless.

```elixir
defmodule MyApp.TenantCase do
  @moduledoc false
  use ExUnit.CaseTemplate

  alias EctoFoundationDB.Sandbox

  setup do
    tenant_id = Ecto.UUID.autogenerate()

    tenant = Sandbox.checkout(MyApp.Repo, tenant_id, [])

    on_exit(fn ->
      Sandbox.checkin(MyApp.Repo, tenant_id)
    end)

    {:ok, [tenant_id: tenant_id, tenant: tenant]}
  end
end
```

Now, you can use your TenantCase to do any FDB operation in a test. Because
we're using tenants with randomized names, you don't have to worry about key conflicts.

```elixir
defmodule MyAppHelloFDBTest do
  use MyApp.TenantCase

  alias Ecto.Adapters.FoundationDB

  test "hello", context do
    tenant = context[:tenant]

    # An :erlfdb hello world, delete this for your tests
    assert :not_found ==
             FoundationDB.transactional(
               tenant,
               fn tx ->
                 tx
                 |> :erlfdb.get("hello world")
                 |> :erlfdb.wait()
               end
             )

    # Example:
    #   MyApp.Repo.insert!(%MyApp.Hello{message: "world"}, prefix: tenant)
  end
end
```
