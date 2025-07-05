# Changelog

## v0.5.0 (TBD)

### Enhancements

* Added ability to drop an index.
* `EctoFoundationDB.CLI`: Functions for an operator to use to safely manage
data migrations that cannot be done automatically.
* Metadata cache is now shared across all open tenants of the same id on a
given node.
* Metadata cache now makes use of FoundationDB's `\xff/metadataVersion` key,
which allows the client to cache metadata and maintain transactional isolation
without having to wait on any keys.
* Added support for versionstamping with `EctoFoundationDB.Versionstamp`.

### Breaking changes

* Key construction has changed, making databases created on <= 0.4.x incompatible with >=0.5. Specifically, a binary, atom, or
  number primary key is now encoded in the FDB key with the Tuple layer. All other types remain encoded with term_to_binary.
  If you need help upgrading your database, please put in a GitHub Issue.

### Deprecations

* Ecto has deprecated `Repo.transaction` in favor of `Repo.transact`. Since this decision doesn't align with FoundationDB's view of
transactions, we have chosen to deprecate `Repo.transaction` in favor of `Repo.transactional`. This terminology better aligns with
`:erlfdb` and provides a distinction from RDBMS transactions, and allows us to avoid future deprecations.

### New documentation

* [Guide for Operators](operators_manual.html): Describes how to use the `EctoFoundationDB.CLI` functions to rename a field while guaraneeting that all
concurrent queries in your distributed application are successful.

## v0.4.0 (2024-01-16)

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
