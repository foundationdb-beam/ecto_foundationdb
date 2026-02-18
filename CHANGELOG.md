# Changelog

## v0.7.1 (2026-02-17)

### Bug fixes

* (#83): Fixed re-entrancy problem when running a migration on a single tenant repo

## v0.7.0 (2026-02-16)

### Enhancements

* A Repo can be configured to be locked to a single tenant by specifying `:tenant_id` config.
* `:key_limit` is a valid Repo option. When specified, the underlying FDB GetRange will be limited to the
  specified number of keys. Note that there is not always a 1-to-1 mapping of keys to objects, due to
  large objects being split across multiple keys.
* Metadata retrieval is now skipped when querying a schema by its primary key only. e.g. `Repo.all(User, ...)`
* Added `FDB.stream_range/4`, currently undocumented, that wraps the `:erlfdb_range_iterator` in a `Stream` for
  easier use in Elixir apps.
* When querying with no where clause and an order_by, an index will now be selected if one exists.
* Improved error messages for unsupported order_by + limit interactions.

### Bug fixes

* A `:limit` in `Ecto.Query` will now work as expected when encountering objects split across multiple keys.
* When reading SchemaMigration versions, only the maximum version number is retrieved, which will result in
  a small performance benefit when opening a Tenant.

### Relevant internal changes

Internally, much of the Query and Future interfaces have been refactored to support the Query limit bug fix.
In particular, we are now using `:erlfdb_range_iterator`, which provides more control over the paged retrieval
of key-values from the database server. We hope these paths are now simpler and easier to maintain over time.

### New documentation

* [Serialization Design](serialization.html): Describes how your data is serialized to FDB values.
* Improvements to implementation traces in `fdb_api_counting_test.exs`

### Dependencies

* `erlfdb ~> 0.3.4`
* Added support for Elixir 1.19 and Erlang 28
* Updated CI FDB to 7.3.69
* Updated all outdated deps

## v0.6.3 (2025-01-12)

### Bug fixes

* Support limit clause from Ecto.Query in addition to limit option on Repo calls.

## v0.6.2 (2025-01-10)

### Bug Fixes

* Prevent race when creating same tenant from multiple processes

## v0.6.1 (2025-12-28)

### Dependencies

* `erlfdb ~> 0.3.3`

## v0.6.0 (2025-12-27)

### Enhancements

* SchemaMetadata now supports indexed fields
* `EctoFoundationDB.Sync`: Defines conventions for keeping a stateful process (e.g. LiveView) automatically up-to-date
  with the latest data from the database.

### Dependencies

* `ecto ~> 3.13`

### New documentation

* [Sync Engine Part III - Batteries Included](sync_module.livemd): New Livebook that demonstrates how to use the `EctoFoundationDB.Sync` module in LiveView.

## v0.5.1 (2025-12-19)

### Enhancements

* Improved support for `order_by` + `limit` queries.
* Caller may provide query to run on assign_ready for SchemaMetadata watches.

## v0.5.0 (2025-07-08)

### Enhancements

* `EctoFoundationDB.CLI` [[doc](operators_manual.md)]: Functions for an operator to use to safely manage
data migrations that cannot be done automatically.
* Index Metadata [[doc](metadata.html)] now makes use of FoundationDB's `\xff/metadataVersion` key,
which allows the client to cache metadata and maintain transactional isolation
without having to wait on any keys. Also, the cache is shared across all open tenants of the same id on a given node.
* `EctoFoundationDB.Versionstamp` [[doc](Ecto.Adapters.FoundationDB.html#module-versionstamps-autoincrement)]: Added the ability to insert objects with a monotonically increasing integer id, via FoundationDB's versionstamp.
* `SchemaMetadata` [[doc](Ecto.Adapters.FoundationDB.html#module-schema-metadata)]: This is a new built-in Indexer that allows your app to watch and sync collection of objects in a tenant.
* Added ability to drop an index.

### Breaking changes

* Key construction has changed, making databases created on <= 0.4.x incompatible with >=0.5. Specifically, a binary, atom, or
  number primary key is now encoded in the FDB key with the Tuple layer. All other types remain encoded with term_to_binary.
  If you need help upgrading your database, please put in a GitHub Issue. We strive for a stable v1.0.

### Bug fixes

* Fixed a bug where index creation was failing for multikey objects
* (#57) Fixed a bug where index management was failing while a new index was being created

### Deprecations

* Ecto has deprecated `Repo.transaction` in favor of `Repo.transact`. Since this decision doesn't align with FoundationDB's view of
transactions, we have chosen to deprecate `Repo.transaction` in favor of `Repo.transactional`. This terminology better aligns with
`:erlfdb` and provides a distinction from RDBMS transactions, and allows us to avoid future deprecations.

### New documentation

* [Guide for Operators](operators_manual.html): Describes how to use the `EctoFoundationDB.CLI` functions to rename a field while guaranteeing that all
concurrent queries in your distributed application are successful.
* [Metadata Design](metadata.html): Describes how index metadata is managed and cached in EctoFDB.
* [Sync Engine Part I - Single Object](watches.livemd): Revamped Livebook that demonstrates how to create a Sync Engine for a single object (for syncing reads)
* [Sync Engine Part II - Collections](collection_syncing.livemd): New Livebook that demonstrates a Sync Engine for a collection of objects in a tenant (still for reads)

## v0.4.0 (2025-01-16)

### Enhancements

* [Large Structs](Ecto.Adapters.FoundationDB.html#module-advanced-options): If your struct encodes to a size larger than 100,000 Bytes, it will now be split across several FDB key-values automatically.
  Previously, EctoFDB did not attempt to detect this and allowed FoundationDB to throw error code 2103: "value_too_large - Value length exceeds limit".
  See documentation for configuration options.
* `EctoFoundationDB.Sandbox` now uses `m:erlfdb_sandbox`. Sandbox directory name is now `.erlfdb_sandbox`. Directories named `.erlfdb` should be removed.

### Bug fixes

* Upgrade erlfdb to v0.2.2

## v0.3.1 (2024-10-24)

### Bug fixes

* Fixed consistency issue with index updates. Previously, the old index key was still queryable.
* Fixed write amplification issue when updating struct's non-indexed fields.

### New Documentation

* Added `fdb_api_counting_text.exs` which tests and documents the `:erlfdb` operations that our Layer is expected to make.

## v0.3.0 (2024-10-20)

### \*\* Major breaking changes \*\*

Databases that have been created using a prior version of EctoFoundationDB will be broken on
EctoFDB v0.3 and above. Please start a new database with EctoFDB v0.3. If you currently have
a database on v0.2 or earlier, please [submit an issue](https://github.com/foundationdb-beam/ecto_foundationdb/issues)
to discuss the upgrade path.

### Enhancements

* [Watches](Ecto.Adapters.FoundationDB.html#module-watches): Support for FDB Watches, which is like a database-driven PubSub on an Ecto struct.
* [Directory Tenants](EctoFoundationDB.Tenant.html): A new default backend for Multitenancy that is production-ready. Managed tenants have been moved to "Experimental" status.
* `@schema_context usetenant: true` is no longer required.
* The `:open_db` option now defines a 1-arity function that accepts the Repo module.

### New Documentation

* [Livebook | Watches in LiveView](watches.livemd)

## v0.2.1 (2024-09-23)

### Bug fixes

  * Upgrade erlfdb to 0.2.1, allowing Livebook Desktop to discover fdbcli location in /usr/local/bin

## v0.2.0 (2024-09-21)

### Bug fixes

### Enhancements

  * [Upserts](Ecto.Adapters.FoundationDB.html#module-upserts): Support for Ecto options `:on_conflict` and `:conflict_target`
  * [Pipelining](Ecto.Adapters.FoundationDB.html#module-pipelining): New Repo functions for async/await within a transaction.

### New Documentation

  * [testing.md](testing.html): Document to describe how to set up a Sandbox
  * [CHANGELOG.md](changelog.html): This file!

## v0.1.2 (2024-08-31)

### Bug fixes

* Upgrade erlfdb

## v0.1.1 (2024-08-25)

### Enhancements

* Upgrade to Ecto 3.12

### New documentation

* [Livebook | Getting Started](introduction.livemd): How to get started with EctoFoundationDB.

## v0.1.0 (2024-04-07)

### Features

* Multitenancy
* Basic CRUD operations
* Indexes
