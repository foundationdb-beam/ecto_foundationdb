# Ecto FoundationDB Adapter

[![CI](https://github.com/foundationdb-beam/ecto_foundationdb/actions/workflows/ci.yml/badge.svg)](https://github.com/foundationdb-beam/ecto_foundationdb/actions/workflows/ci.yml)

EctoFoundationDB makes using FoundationDB in Elixir easy.

We provide an Ecto Adapter for FoundationDB, written using 
[foundationdb-beam/erlfdb](https://github.com/foundationdb-beam/erlfdb)
as the driver for communicating with FoundationDB. As FDB is a distributed
key-value store, higher level data models are implemented
as client-side [Layers](https://apple.github.io/foundationdb/layer-concept.html).
EctoFDB is a sophisticated layer that implements the Ecto API.

## Design principles

EctoFDB keeps the control in the developer's hands. You know exactly how you want
to access your data, and you want predictable query performance.
We map the Ecto operations to the FDB API in a predictable and well-documented
fashion, so you can maintain full control. You won't be caught by surprise by any
slow queries.

And if you need to dive into raw key-value operations, it's right there for you,
right alongside your Ecto Schemas in the same transaction.

## Key features

* **Scalable Transactions:** Full support for FoundationDB’s serializable transactions.
* **Built-in Multitenancy:** First-class support for tenants via EctoFoundationDB.Tenant, allowing for logically partitioned databases with zero execution overhead.
* **Rich Indexing:** Support for default, timestamp, and multi-field indexes. You can even define custom indexers.
* **Online Migrations:** Managed schema evolution designed for large-scale distributed systems.
* **Advanced Features:**
    * **Pipelining:** Send multiple queries simultaneously and await them to reduce network round trips.
    * **Watches:** Register "push-style" notifications on keys to trigger Elixir process updates immediately when data changes.
    * **Large Structs:** Automatically handles FoundationDB's 100KB value limit by splitting large binaries across multiple keys behind the scenes.

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

## Usage

See the [documentation](https://hexdocs.pm/ecto_foundationdb) for usage
information.

## Development Philosophy & AI Disclosure

This project is **Human-Architected**. While we leverage modern tools, over
90% of this codebase is written manually, with a strict focus on intentional
logic and security over high-speed generation. AI is used sparingly (less than 10%)
as a specialized tool for boilerplate and syntax completion—never for core
architecture or decision-making. In fact, in the spirit of full transparency,
we’ll acknowledge the irony: this very paragraph is one of the few pieces of
AI-assisted content in this repository. It was used here to ensure this disclosure
is as clear and professional as possible, while the code it describes remains a
product of human craftsmanship.
