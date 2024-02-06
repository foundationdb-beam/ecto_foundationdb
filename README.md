# EctoFoundationDB

**Work in progress. Almost nothing works. Please see unit tests to see what's currently supported.**

## Driver

An Ecto Adapter for FoundationDB, written using [apache/couchdb-erlfdb](https://github.com/apache/couchdb-erlfdb).
`ecto_foundationdb` is currently configured to use a temporary fork of `erlfdb` at
[JesseStimpson/erlfdb](https://github.com/JesseStimpson/erlfdb). The fork includes support for
FDB's [Tenants](https://apple.github.io/foundationdb/tenants.html).

## Goals

Our goal is to create a simple and straight-forward adapter that's able to take advantage of FDB's
strengths, including:

* Tenants
* getrange and clearrange
* Transactions

It's unlikely that this project will be able to support the full Ecto featureset due to the nature
of FoundationDB as a key-value store. While it's possible to layer full SQL semantics on a FoundationDB
cluster, this project doesn't attempt to do so. This means that certain Ecto features will stay unimplemented.
We'll call these things out explicitly as they become clear.

## Contributing

Contributions are welcome. I've found the following resources helpful during development:

* [ecto_mnesia](https://github.com/Nebo15/ecto_mnesia)
* [ecto_qlc](https://github.com/Schultzer/ecto_qlc)
* [ecto_sql](https://github.com/elixir-ecto/ecto_sql)
* [ecto_sqlite3](https://github.com/elixir-sqlite/ecto_sqlite3)
* [Source of the ecto Adapter behaviours](https://github.com/elixir-ecto/ecto/tree/master/lib/ecto/adapter)

## Installation

Do not install this as a depedency for your application. It's not ready for that.
