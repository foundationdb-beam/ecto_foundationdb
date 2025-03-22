# Metadata

EctoFDB stores metadata alongside your Ecto schema data in order to maintain
correct data integrity on the indexes. In doing so, each EctoFDB client
knows which indexes are needed to be written to, even if the application code
is on an older version, which can be the case when deploying updates to a
distributed system.

Since this metadata is required to be available in every transaction, EctoFDB
must *allow for* the retrieval of the metadata in every transaction. However,
we wish to avoid that retrieval from actually happening as much as possible.
Otherwise, those particular keys become very "hot" and limit the scalability
of the system. Therefore, we maintain a local cache of the metadata for each
(tenant, schema) tuple.

This document describes the design considerations for the metadata and the
approaches we use to ensure the cache is guaranteed to be valid.

## Metadata Content

We currently keep track of 2 types of metadata: indexes and partial indexes.

### Indexes

This is a list of all 'ready' indexes for a particular Schema. An index is
considered 'ready' when it has been created and is ready to be used.

At query time, this list of indexes is inspected to determine the index to use
to provide the best possible query performance.

At insert and update time, the list is used to set or clear the index keys
according to the specified `EctoFoundationDB.Indexer`.

### Partial Indexes

This is a list of all indexes that are currently undergoing a migration,
usually for index creation. The creation of an index for a particular
(tenant, schema) tuple can take arbitrarily long, and so any queries that
arrive in the meantime must be handled accordingly. The list of partial
indexes informs EctoFDB of how to keep data integrity for any concurrent
creation of `Default` indexes.

## Metadata Cache

The cache uses 2 stages for invalidation.

* **Stage 1:** The global FoundationDB metadata version key
* **Stage 2:** A version key for each (tenant, schema) tuple, and a special
"claim key" for any partial indexes

#### Metadata Version Key

This is a key that has special treatment in the implementation of
FoundationDB transactions. The
[FDB Design Doc | Metadata Version](https://github.com/apple/foundationdb/blob/deda04b8453ecbc6411cc7ac41efb3213e18343f/design/metadata-version.md)
provides a detailed explanation of how this key is implemented.

For the purposes of this document, it's important to understand that the metadata version key is truly global to the entire keyspace. Since we allow
tenants to migrate independently, this necessarily means that the migration for
a single tenant will invalidate Stage 1 of the cache for **all tenants** in
the  database.

There is no cost to reading the global metadata version key, since it's
always sent along with other necessary transactional data. This means that
at steady state, your transactions only need to do the base minimum FDB
operations, and are able to avoid any hot keys.

If the global version is found to have changed, we do not yet invalidate the
cache. Instead, we move onto Stage 2 of the cache invalidation process. In
doing so, we limit the impact of another tenant's migration.

#### Schema Migration Version Key

This is a key that is managed by EctoFDB itself, and does not have special
treatment by FDB. It alaways contains an integer value representing the largest
version number for complete migrations that are specified in your
`EctoFoundationDB.Migrator`.

When the global key is found to have changed in Stage 1 of the cache
invalidation, we perform a 'get' on this key. Then we delay the wait until
after some of the transaction work has been completed. Thus, the cache
is *optimistic* that the cached value is still valid. Only at the end of the
transaction do we wait for and compare the version value. If it's not equal,
then the cache is invalidated and the transaction is retried.

#### Claim Key

This key is used for tracking metadata that is currently undergoing a
migration. It's managed in the same way as the schema migration version key.
It's content includes a cursor that defines the progress of the migration.
