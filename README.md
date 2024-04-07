# Ecto FoundationDB Adapter

[![CI](https://github.com/foundationdb-beam/ecto_foundationdb/actions/workflows/ci.yml/badge.svg)](https://github.com/foundationdb-beam/ecto_foundationdb/actions/workflows/ci.yml)

## Driver

An Ecto Adapter for FoundationDB, written using [foundationdb-beam/erlfdb](https://github.com/foundationdb-beam/erlfdb)
as the driver for communicating with FoundationDB.

## Installation

Install the latest stable release of FoundationDB from the
[official FoundationDB Releases](https://github.com/apple/foundationdb/releases).

You will only need to install the `foundationdb-server` package if you're
running an instance of the FoundationDB Server. For example, it's common to
run the `foundationdb-server` on your development machine and on managed
instances running a FoundationDB cluster, but not for your stateless Elixir
application server in production.

`foundationdb-clients` is always required.

Include `:ecto_foundationdb` in your list of dependencies in `mix.exs`:

```elixir
defp deps do
  [
    {:ecto_foundationdb, git: "https://github.com/foundationdb-beam/ecto_foundationdb.git", branch: "main"}
  ]
end
```

## Usage

See the [documentation](https://hexdocs.pm/ecto_foundationdb) for usage
information.

### Tenants

`ecto_foundationdb` requires the use of FoundationDB Tenants, which can be
enabled on your cluster with the following configuration in an `fdbcli` prompt.

```
fdb> configure tenant_mode=optional_experimental
```

## Running tests

To run the integration tests, use the following.

```sh
mix test
```
