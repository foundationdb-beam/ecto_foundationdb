# Changelog for v0.x

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
