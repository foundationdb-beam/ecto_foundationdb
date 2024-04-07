defmodule Ecto.Adapters.FoundationDB.Layer do
  @moduledoc """
  The Ecto FoundationDB Adapter implements a [Layer](https://apple.github.io/foundationdb/layer-concept.html)
  on the underlying key-value store provided by FoundationDB. Via this layer, some
  common data access patterns are achieved. For those familiar with relational databases
  such as Postgres, these patterns will be familiar. However, there are many differences (for example SQL is
  not supported), so this document seeks to describe the capabilities of the Ecto FoundationDB Layer in detail.

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

  The User can be inserted and retrieved:

    iex> tenant = Tenant.open!(Repo, "an-org-id")
    iex> user = Repo.insert!(%User{name: "John", department: "Engineering"}, prefix: tenant)
    iex> Repo.get!(User, user.id, prefix: tenant)

  Within a tenant, all objects from your Schema can be retrieved at once.

    iex> Repo.all(User, prefix: tenant)
    [%User{id: "some-uuid", name: "John", department: "Engineering"}]

  Note: The Primary Write can be skipped by providing the `write_primary: false` option on the `@schema_context`.
  See below for more.

  ## Index Write and Read

  As shown above, you can easily get all Users in a tenant. However, say you wanted to
  get all Users from a certain department with high efficiency.

  Via an Ecto Migration, you can specify a Default index on the `:department` field.

  ```elixir
  defmodule MyApp.Migration do
    use Ecto.Adapters.FoundationDB.Migration
    def change() do
      [create(index(User, [:department]))]
    end
  end
  ```

  When this index is created via the migration, the Ecto FoundationDB Adapter writes a set of
  keys and values to facilitate lookups based on the indexed field.

    iex> query = from(u in User, where: u.department == ^"Engineering")
    iex> Repo.all(query, prefix: tenant)

  The index value can be any Elixir term. All types support Equal queries. However, certain Ecto Schema types
  support Between queries. For example, you can define an index on a naive_datetime_usec field to construct
  an effective time series index.

  iex> query = from(e in Event,
  ...>   where: e.timestamp >= ^~N[2024-01-01 00:00:00] and e.timestamp < ^~N[2024-01-01 12:00:00]
  ...> )
  iex> Repo.all(query, prefix: tenant)

  ### Multiple index fields

  **Order matters!**: When you create an index using multiple fields, the FDB key that stores the index will be extended with
  all the values in the order of your defined index fields. For example, you may want to have a time series index, but also
  have the ability to easily drop all events on a certain date. You can achieve this by creating an index on `[:date, :user_id, :time]`.
  The order of the fields determines the Between queries you can perform.

  ```elixir
  defmodule MyApp.Migration do
    use Ecto.Adapters.FoundationDB.Migration
    def change() do
      [create(index(Event, [:date, :user_id, :time]))]
    end
  end
  ````

  With this index, the following queries will be efficient:

  ```elixir
  iex> query = from(e in Event,
  ...>   where: e.date == ^~D[2024-01-01] and
                e.user_id == ^"user-id" and
                e.time >= ^~T[12:00:00] and e.time < ^~T[13:00:00]
  ...> )

  iex> query = from(e in Event,
  ...>   where: e.date >= ^~D[2024-01-01] and e.date < ^~D[2024-01-02]
  ...> )
  ```

  However, this query will raise a Runtime exception:

  ```elixir
  iex> query = from(e in Event,
  ...>   where: e.date >= ^~D[2024-01-01] and e.date < ^~D[2024-01-02]
                e.user_id == ^"user-id"
  ...> )
  ```

  There can be 0 or 1 Between clauses, and if one exists, it must correspond to the final constraint in the where clause when
  compared against the order of the index fields.

  ### Advanced Options: `write_primary: false`, `mapped?: false`

  If you choose to use `write_primary: false` on your schema, this skips the Primary Write. The consequence of this are as follows:

   1. You'll want to make sure your index is created with an option `mapped?: false`. This ensures that the
      struct data is written to the index keyspace.
   2. Your index queries will now have performance characteristics similar to a primary key query. That is, you'll
      be able to retrieve the struct data with a single `erlfdb:get_range/3`.
   3. The data can **only** be managed by providing a query on the index. You will not be able to access the data
      via the primary key.
   4. If `write_primary: false`, then only one index can be created.

  ## User-defined Indexes

  The Ecto FoundationDB Adapter also supports user-defined indexes. These indexes are created and managed
  by your application code. This is useful when you have a specific query pattern that is not covered by
  the Default index or Time Series index. Internally, MaxValue is an example of a user-defined index that
  the adapter uses to manage the schema_migrations and index caching.

  To create a user-defined index, you must define a module that implements the Indexer behaviour.

  Please see Ecto.Adapters.FoundationDB.Indexer for more information, and Ecto.Adapters.FoundationDB.Indexer.MaxValue
  for an example implementation.

  ```elixir

  ## Transactions

  The Primary and Index writes are guaranteed to be consistent due to FoundationDB's
  [ACID Transactions](https://apple.github.io/foundationdb/developer-guide.html#transaction-basics).

  As the application developer, you can also take advantage of Transactions to implement your own
  data access semantics. For example, if you wanted to make sure that when a user is inserted, an
  event is recorded of the operation, it can be done via a Transaction.

    iex> fun = fn ->
    ...>   Repo.insert!(%User{name: "John"})
    ...>   Repo.insert!(%Event{timestamp: ~N[2024-02-18 12:34:56], data: "Welcome John"})
    ...> end
    iex> Repo.transaction(fun, prefix: tenant)

  """
end
