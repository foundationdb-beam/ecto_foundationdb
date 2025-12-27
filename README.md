# Ecto FoundationDB Adapter

[![CI](https://github.com/foundationdb-beam/ecto_foundationdb/actions/workflows/ci.yml/badge.svg)](https://github.com/foundationdb-beam/ecto_foundationdb/actions/workflows/ci.yml)

## Driver

An Ecto Adapter for FoundationDB, written using [foundationdb-beam/erlfdb](https://github.com/foundationdb-beam/erlfdb)
as the driver for communicating with FoundationDB.

## Installation

Install the latest stable release of FoundationDB from the
[official FoundationDB Releases](https://github.com/apple/foundationdb/releases).

The `foundationdb-server` package is required on any system that will be running
a FoundationDB server instance. For example, it's common to
run the `foundationdb-server` on your development machine and on managed
instances running a FoundationDB cluster, but not for your stateless Elixir
application server in production.

`foundationdb-clients` is always required.

Include `:ecto_foundationdb` in your list of dependencies in `mix.exs`:

```elixir
defp deps do
  [
    {:ecto_foundationdb, "~> 0.6"}
  ]
end
```

## Motivation

What are some reasons to choose EctoFDB?

FoundationDB offers:

* Horizontal scaling of high-write workloads
* Unbounded multi-tenancy
* Serializable Transactions
* Rich operations: multi-region, disaster recovery, backup/restore, telemetry

EctoFoundationDB offers:

* Object storage similar to [Record Layer](https://github.com/FoundationDB/fdb-record-layer)
* Online migrations
* Built-in common indexes
* Extensible index types

## Usage

See the [documentation](https://hexdocs.pm/ecto_foundationdb) for usage
information.

For documentation on `main` branch, see [Ecto.Adapters.FoundationDB](https://github.com/foundationdb-beam/ecto_foundationdb/blob/main/lib/ecto/adapters/foundationdb.ex).

## Running tests

To run the integration tests, use the following.

```sh
mix test
```
