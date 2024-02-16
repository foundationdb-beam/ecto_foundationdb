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
user = %User{name: "John"}
       |> FoundationDB.usetenant(tenant)

MyApp.Repo.insert!(user)
```

**Note**: Because we are using the "some-org" tenant, John is 
considered a member of "some-org" without having to specify the relationship
in the User schema itself.

Querying for a struct using the primary key.

```elixir
MyApp.Repo.get!(User, user.id, prefix: tenant)
```

Failure to specify a tenant will result in a raised exception at runtime.

## Layer

Because FoundationDB is key-value store, support for typical data access patterns must be
implemented as a [Layer](https://apple.github.io/foundationdb/layer-concept.html) that sits on top
of the underlying data store.

The layer implemented in `ecto_foundationdb` shares some features with the official
[Record Layer](https://github.com/FoundationDB/fdb-record-layer); however, we have not yet
endeavored to implement it. A mechanism to introduce pluggable Layers into this adapter is on the
roadmap.

The `ecto_foundationdb` Layer supports the following data access patterns.

* **Set/get/delete**: Write and read a Struct based on a unique primary key.
* **Get all**: Retrieval of all Structs in a given tenant.
* **Simple index**: Retrieval of all Structs in a tenant that match a particular field value. The index must be created ahead of time using a migration.
* **Timeseries**: Retrieval of all Structs in a tenant that include a timestamp in between a given timespan. The timeseries index must be created ahead of time using a migration.

## Running tests

To run the integration tests, use the following.

```sh
mix test
```

## Features

Roughly in order of priority.

 - [x] Sandbox
 - [x] Basic crud operations
 - [x] Single index
 - [x] Initial documentation
 - [x] Time series index (auto gen pk, optionally skip primary write)
 - [ ] Migration tooling (handling many tenants)
 - [ ] Migration locking
 - [ ] Layer isolation and docs
 - [ ] Hierarchical multi index
 - [ ] Code cleanliness/readability (esp Tx module)
 - [ ] FDB Watches
 - [ ] Benchmarking
 - [ ] Ecto Transactions
 - [ ] `TestRepo.stream`
 - [ ] Migration logging
 - [ ] Composite primary key
 - [ ] Pluggable Layers
