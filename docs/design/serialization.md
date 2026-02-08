# Serialization Design

EctoFDB stores each entity as a single FDB value using Erlang's External Term
Format (`:erlang.term_to_binary/1`). The input is a keyword list where each key
is an atom corresponding to a column name and each value is the column's Elixir
term. For example:

```elixir
[id: "abc-123", name: "Alice", notes: nil, inserted_at: ~N[...], updated_at: ~N[...]]
```

This document describes why this encoding was chosen and the tradeoffs involved.

## `Ecto.Schema` as the source of truth

Because every row carries its own column names, EctoFDB does not need to store a
schema definition as a separate metadata entry in FoundationDB, and does not
require a `create table` migration statement. Instead, the
`Ecto.Schema` module in the application code is the sole source of truth for
the shape of the data.

This is a natural fit for FoundationDB's client-driven Layer architecture: the
client is always in control of how data is interpreted. Avoiding a stored schema
eliminates an entire class of coordination problems:

* No schema metadata key to read on every transaction.
* No cache invalidation surface for schema changes.
* No versioned schema mappings to maintain.

The `Metadata` system (see `metadata.md`) already handles index metadata and
cache invalidation. Keeping the value encoding free of metadata dependencies
keeps the common read/write path simple.

## Column Evolution

Adding a column requires only a code change to the `Ecto.Schema`. Existing
rows that predate the new column will decode without it, and the missing key
resolves to `nil` at the Ecto layer. No backfill migration is needed.

Renaming a column requires rewriting all existing rows, since the old atom name
is embedded in each value. This is consistent with the general expectation that
column renames are rare and expensive operations.

In practice, the tenant model eases this during development: rather than
migrating a tenant's data through a rename, it is often simpler to delete
the tenant's data and let it repopulate from scratch. This works well when
tenants are cheap to recreate and the authoritative data lives elsewhere or
can be reseeded.

## Why `term_to_binary`?

`:erlang.term_to_binary/1` is implemented as a single C-level BIF in the BEAM.
It handles the entire keyword list (atoms, strings, integers, floats,
timestamps, nested structures) in one call with no Elixir-level dispatch per
field. It's well optimized, portable, simple, reliable, and well-trodden.

We benchmarked an alternative encoding that strips atom keys out of the binary
and stores them as a NUL-delimited string header, with a single
`:erlang.term_to_binary` call for the values list:

```
<<atom1, 0x00, atom2, 0x00, ..., atomN, 0x00, 0x00, term_to_binary(values_list)>>
```

Results across representative row shapes (narrow 2-field rows, typical 5-field
rows, wide 12-field rows, and content-heavy 9-field rows):

| Metric         | Custom vs. Baseline |
|----------------|---------------------|
| Size savings   | 1-7%                |
| Encode speed   | 1.2x slower         |
| Decode speed   | 1.6-2.7x slower     |

The size savings are modest because Erlang's External Term Format already
encodes atoms compactly (a small tag followed by the atom bytes). The
performance cost comes from the Elixir-level header parsing and per-field
`String.to_existing_atom` calls on decode, which cannot compete with the
BEAM's native `binary_to_term`.

## Storage Overhead

Each atom key costs roughly 10-15 bytes per row in the serialized form. For a
typical schema with 5-8 fields, this is 50-120 bytes of overhead per entity.
The overhead as a percentage of total row size depends on the data:

* **Narrow rows** (2-3 small fields): ~40-60% overhead
* **Typical rows** (5-8 fields, moderate values): ~15-30%
* **Content-heavy rows** (large string fields): <5%

This overhead is not free. Larger values reduce the number of rows that fit in
a single FDB transaction (10 MB limit), which matters for bulk data loading
scenarios. For narrow rows with high atom overhead, the effective throughput per
transaction can be noticeably lower than it would be with a more compact
encoding.

## Comparison with Other Systems

The schema-per-row approach is similar to DynamoDB's approach:

* **[DynamoDB](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/CapacityUnitCalculations.html)**
  stores attribute names with every item, for the same reasons: schemaless
  flexibility and no coordination on schema changes.

Some alternative approaches include:

* **[FoundationDB Record Layer](https://www.foundationdb.org/files/record-layer-paper.pdf)**
  (Java) uses Protocol Buffers, which replace field names with small integer
  tags. This is more space-efficient but requires a compiled schema definition
  and careful field-number evolution rules.
* **[CockroachDB](https://github.com/cockroachdb/cockroach/blob/master/docs/tech-notes/encoding.md)**
  uses small integer column-ID deltas in a compact TUPLE encoding within each
  column family's value. Column names are not stored per row; the schema maps
  IDs to names.
* **[Spanner](https://cloud.google.com/blog/products/databases/spanner-modern-columnar-storage-engine)**
  uses an internal columnar/PAX storage format (Ressi) where column identity is
  part of the schema metadata, not repeated per row.

EctoFDB's approach trades a moderate, predictable storage overhead for a
significantly simpler architecture with no stored schema coordination.

## Future Direction: Integer-Keyed Encoding

**This strategy is not yet implemented.**

A potential optimization is to replace atom keys with user-assigned integers,
similar to Protocol Buffer field numbers. Instead of:

```elixir
:erlang.term_to_binary([id: "abc-123", name: "Alice"])
```

The encoding would be:

```elixir
:erlang.term_to_binary([{0, "abc-123"}, {1, "Alice"}])
```

Benchmarks show this yields 8-15% size savings on typical rows, with encode
performance roughly on par with the current approach and decode at break-even.

This would be opt-in via a schema annotation, for example:

```elixir
schema "users" do
  field :name, :string, fdb_key: 1
  field :notes, :string, fdb_key: 2
  timestamps()
end
```

If no `fdb_key` annotations are present, the current keyword-list encoding would be
used. The decoder can disambiguate the two formats by inspecting the
deserialized term: a keyword list (atom-keyed tuples) vs integer-keyed tuples.

The user would own the stability of the integer mapping, the same contract as
protobuf field numbers: once assigned, an integer must never be reassigned to a
different field. This is a reasonable burden because it enables two benefits
beyond size savings:

* **Column renames become free.** Renaming `:name` to `:full_name` requires
  only a schema change; the integer key `1` stays the same, so existing data
  needs no migration, assuming all records were stored with this scheme.
* **No stored metadata.** The `Ecto.Schema` module remains the sole source of
  truth for the field-to-integer mapping, preserving the architecture described
  in this document.
