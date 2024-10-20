# Changelog for v0.x

## v0.3 (2024-10-20)

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

  * [TESTING.md](testing.html): Document to describe how to set up a Sandbox
  * [CHANGELOG.md](changelog.html): This file!
