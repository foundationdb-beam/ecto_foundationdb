# Ecto FoundationDB Adapter

**Work in progress. Please see integration tests to see what's currently supported.**

## Driver

An Ecto Adapter for FoundationDB, written using [apache/couchdb-erlfdb](https://github.com/apache/couchdb-erlfdb)
as the driver for communicating with FoundationDB.

Note: `ecto_foundationdb` is currently configured to use a temporary fork of `erlfdb` at
[JesseStimpson/erlfdb](https://github.com/JesseStimpson/erlfdb). The fork includes support for
FDB's [Tenants](https://apple.github.io/foundationdb/tenants.html).

## Installation

`ecto_foundationdb` is still under development; it's not ready for any production workloads.

```elixir
defp deps do
  [
    {:ecto_foundationdb, git: "https://github.com/JesseStimpson/ecto_foundationdb.git", branch: "main"}
  ]
end
```

## Usage

Define your repo similar to this.

```elixir
defmodule MyApp.Repo do
  use Ecto.Repo, otp_app: :my_app, adapter: Ecto.Adapters.FoundationDB
end
```

Configure your repo similar to the following.

```elixir
config :my_app,
  ecto_repos: [MyApp.Repo]

config :my_app, MyApp.Repo,
  cluster_file: "/etc/foundationdb/fdb.cluster"
```

### Tenants

`ecto_foundationdb` requires the use of FoundationDB Tenants, which can be enabled on your cluster
with the following configuration in an `fdbcli` prompt.

```
fdb> configure tenant_mode=optional_experimental
```

Creating a schema to be used in a tenant.

```elixir
defmodule User do
  use Ecto.Schema
  @schema_context usetenant: true
  # ...
end
```

Setting up a tenant.

```elixir
alias Ecto.Adapters.FoundationDB
alias Ecto.Adapters.FoundationDB.Tenant

tenant = Tenant.open!(MyApp.Repo, "some-org")
```

Each call on your `Repo` must specify a tenant via the Ecto `prefix` option.

Inserting a new struct.

```elixir
user = %User{name: "John", organization_id: "some-org"}
       |> FoundationDB.usetenant(tenant)

MyApp.Repo.insert!(user)
```

Querying for a struct using the primary key.

```elixir
MyApp.Repo.get!(User, user.id, prefix: tenant)
```

Failure to specify a tenant will result in a raised exception at runtime.

## Running tests

To run the integration tests, use the following.

```sh
mix test
```

## Features

Roughly in order of priority.

[x] Sandbox
[x] Basic crud operations
[x] Single index
[x] Initial documentation
[ ] Migration tooling (handling many tenants)
[ ] Migration locking
[ ] Layer documentation
[ ] Hierarchical multi index
[ ] Code cleanliness/readability (esp Tx module)
[ ] FDB Watches
[ ] Benchmarking
[ ] Ecto Transactions
[ ] `TestRepo.stream`
[ ] Migration logging
[ ] Composite primary key
[ ] Pluggable Layers
