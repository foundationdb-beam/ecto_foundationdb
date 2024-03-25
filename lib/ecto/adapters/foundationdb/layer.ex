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
  See Time Series Index for more.

  ## Index Write and Read

  As shown above, you can easily get all Users in a tenant. However, say you wanted to
  get all Users from a certain department with high efficiency.

  Via an Ecto Migration, you can specify a Default index on the `:department` field.

  ```elixir
  defmodule MyApp.Migration do
    use Ecto.Adapters.FoundationDB.Migration
    def change() do
      [create(index(:users, [:department]))]
    end
  end
  ```

  When this index is created via the migration, the Ecto FoundationDB Adapter writes a set of
  keys and values to facilitate lookups based on the indexed field.

    iex> query = from(u in User, where: u.department == ^"Engineering")
    iex> Repo.all(query, prefix: tenant)

  The index value can be any Elixir term.

  Suggestion: Before you create an index, we suggest you test your workload without the index first. You may
  be surprised by the efficiency in which `Repo.all(User, prefix: tenant)` can return data. Once you have
  the data, you can do sophisticated filtering within Elixir itself.

  ## Time Series Index

  This is a special kind of Default index that requires the indexed value to be `:naive_datetime_usec`. This
  index allows a query to retrieve objects that have a datetime that exists in between two given endpoints
  of a timespan.

  ```elixir
  defmodule EctoFoundationDB.Schemas.Event do
    use Ecto.Schema
    @schema_context usetenant: true, write_primary: false
    schema "events" do
      field(:timestamp, :naive_datetime_usec)
      field(:data, :string)
      timestamps()
    end
  end
  ```

  ```elixir
  defmodule EctoFoundationDB.Migration do
    use Ecto.Adapters.FoundationDB.Migration
    def change() do
      [create(index(:events, [:timestamp], options: [indexer: :timeseries]))]
    end
  end
  ```

  Take note of the option `indexer: :timeseries` on the index creation in the Migration module.

  Also notice that in the Schema, we choose to use `write_primary: false`. This skips the Primary Write.
  However, this means that the data can **only** be managed by providing a timespan query. It also means
  that indexes cannot be created in the future, because indexes are always initialized from the Primary Write.

    iex> query = from(e in Event,
    ...>   where: e.timestamp >= ^~N[2024-01-01 00:00:00] and e.timestamp < ^~N[2024-01-01 12:00:00]
    ...> )
    iex> Repo.all(query, prefix: tenant)

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
    iex> FoundationDB.transactional(tenant, fun)

  For now, this Transaction lives separate from Ecto's own Transaction, so please be mindful when using
  this feature to use `Ecto.Adapters.FoundationDB.transactional`.

  """
end
