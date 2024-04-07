defmodule EctoFoundationDB.Layer do
  @moduledoc """
  The Ecto FoundationDB Adapter implements a [Layer](https://apple.github.io/foundationdb/layer-concept.html)
  on the underlying key-value store provided by FoundationDB. Via this layer, some
  common data access patterns are achieved. For those familiar with relational databases
  such as Postgres, these patterns will be familiar. However, there are many differences (for example SQL is
  not supported), so this document seeks to describe the capabilities of the Ecto FoundationDB Layer in detail.

  ## Keyspace Design

  All keys used by `:ecto_foundationdb` are encoded with [FoundationDB's Tuple Encoding](https://github.com/apple/foundationdb/blob/main/design/tuple.md)
  The first element of the tuple is a string prefix that is intender to keep the `:ecto_foundationdb` keyspace
  separate from other keys in the FoundationDB cluster.

  Your Schema data is stored with prefix "\\xFD".

  The data associated with schema migrations is stored with prefix "\\xFE".

  All values are either
    * other keys (in the case of Default indexes) or
    * Erlang term data encoded with `:erlang.term_to_binary/1`

  ## Primary Write and Read

  Your Ecto Schema has a primary key field, which is usually a string or an integer. This primary
  key uniquely identifies an object of your schema within the tenant in which it lives.

  ```elixir
  defmodule EctoFoundationDB.Schemas.User do
    use Ecto.Schema
    @schema_context usetenant: true
    schema "users" do
      field(:name, :string)
      field(:department, :string)
      timestamps()
    end
  end
  ```

  In this example, a User has an `:id` and a `:name`. Also notice that the User is defined with
  `usetenant: true` which provides a scope under which the User lives. For example, a typical tenant
  would be the organization the User belongs to. Since the User is in this tenant, we do not need
  to provide an identifier for this organization on the User object itself.

  "Primary Write" refers to the insertion of the User struct into the FoundationDB key-value store
  under a single key that uniquely identifies the User. This key includes the `:id` value.

  Your struct data is stored as a `Keyword` encoded with `:erlang.term_to_binary/1`.

  Note: The Primary Write can be skipped by providing the `write_primary: false` option on the `@schema_context`.
  See below for more.

  ## Default Indexes

  When a Default index is created via a migration, the Ecto FoundationDB Adapter writes a set of
  keys and values to facilitate lookups based on the indexed field.

  ## Advanced Options: `write_primary: false`, `mapped?: false`

  If you choose to use `write_primary: false` on your schema, this skips the Primary Write. The consequence of this are as follows:

   1. You'll want to make sure your index is created with an option `mapped?: false`. This ensures that the
      struct data is written to the index keyspace.
   2. Your index queries will now have performance characteristics similar to a primary key query. That is, you'll
      be able to retrieve the struct data with a single `erlfdb:get_range/3`.
   3. The data can **only** be managed by providing a query on the index. You will not be able to access the data
      via the primary key.
   4. If `write_primary: false`, then only one index can be created.
  """
end
