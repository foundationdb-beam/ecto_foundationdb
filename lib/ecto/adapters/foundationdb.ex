defmodule Ecto.Adapters.FoundationDB do
  @moduledoc """
  Adapter module for FoundationDB.

  It uses `:erlfdb` for communicating to the database.

  ## Standard Options

    * `:cluster_file` - The path to the fdb.cluster file. The default is
      `"/etc/foundationdb/fdb.cluster"`.
    * `:migrator` - A module that implements the `Ecto.Adapters.FoundationDB.Migrator`
      behaviour. Required when using any indexes. Defaults to `nil`.

  ## Advanced Options

    * `:storage_id` - All tenants created by this adapter are prefixed with
      this string. This allows multiple configurations to operate on the
      same FoundationDB cluster indepedently. Defaults to
      `"Ecto.Adapters.FoundationDB"`.
    * `:open_db` - 0-arity function used for opening a reference to the
       FoundationDB cluster. Defaults to `:erlfdb.open(cluster_file)`. When
       using `Ecto.Adapters.FoundationDB.Sandbox`, you should consider setting
       this option to `Sandbox.open_db/0`.
    * `:migration_step` - The maximum number of keys to process in a single transaction
      when running migrations. Defaults to `1000`. If you use a number that is
      too large, the FDB transactions run by the Migrator will fail.

  ## Limitations and caveats

  There are some limitations when using Ecto with FoundationDB.

  ### Tenants

  As discussed in the README, we require the use of tenants. When a struct is retrieved
  from a tenant (using `:prefix`), that's struct's metadata holds onto the tenant
  reference. This helps to protect your application from a struct accidentally
  crossing tenant boundaries due to some unforeseen bug.

  ### Data Types

  `ecto_foundationdb` stores your struct's data using `:erlang.term_to_binary/1`, and
  retrieves it with `:erlang.binary_to_term/1`. As such, there is no data type
  conversion between Elixir types and Database Types. Any term you put in your
  struct will be stored and later retrieved.

  Data types are used by `ecto_foundationdb` for the creation and querying of indexes.
  Certain Ecto types are encoded into the FDB key, which allows you to formulate
  Between queries on indexes of these types:

   - `:id`
   - `:binary_id`
   - `:integer`
   - `:float`
   - `:boolean`
   - `:string`
   - `:binary`
   - `:date`
   - `:time`
   - `:time_usec`
   - `:naive_datetime`
   - `:naive_datetime_usec`
   - `:utc_datetime`
   - `:utc_datetime_usec`

  See Queries for more information.

  ### Key and Value Size

  FoundationDB imposes strict limits on the size of keys and the size of values. Please
  be aware of these limitations as you develop. `ecto_foundationdb` doesn't make
  any explicit attempt to protect you from these errors.

  ### Layer

  `ecto_foundationdb` implements a specific Layer on the FoundationDB
  key-value store. This Layer is intended to be generally useful, but you
  may not find it suitable for your workloads. The Layer is documented in
  detail at `Ecto.Adapters.FoundationDB.Layer`. This project does not support
  plugging in other Layers.

  ### Queries

  The Layer implemenation affords us a limited set of query types. Any queries
  beyond these will raise an exception. If you require more complex queries,
  we suggest that you first extract all the data that you need using a supported
  query and then constrain, aggregate, and group as needed with Elixir functions.

  In other words, you will be writing less SQL and more Elixir.

    * `Repo.get` using primary key
    * `Repo.all` using no constraint. This will return all such structs for
      the tenant.
    * `Repo.all` using an Equal constraint on an index field. This will return
      matching structs for the tenant.

      ```elixir
      from(u in User, where: u.name == ^"John")
      ```
    * `Repo.all` using a Between constraint on a compatible index field. This will
      return structs in the tenant that have a timestamp between the given timespan
      in the query.

      ```elixir
      from(e in Event,
        where:
          e.timestamp > ^~N[1970-01-01 00:00:00] and
            e.timestamp < ^~N[2100-01-01 00:00:00]
      )
      ```

  ### Indexes

  Simple indexes and time series indexes are supported out of the box. They are
  similar to Ecto SQL indexes in some ways, but critically different in others.

  The out of the box indexes are called Default indexes, corresponding to the module
  by the same name.

    1. An index is created via a migration file, as it is with Ecto SQL. However,
       this is the only supported purpose of migration files so far. And this is
       where the similarities with Ecto SQL end.

    2. Indexes are managed within transactions, so that they will always be
       consistent.

    3. Upon index creation, each tenant's data will be indexed in a stream of FDB
       transactions. This stream maintains transactional isolation for each tenant
       as they migrate. See `ProgressiveJob` for more.

    4. Migrations must be executed on a per tenant basis, and they can be
       run in parallel. Migrations are managed automatically by this adapater.

  ### Transactions

  `ecto_foundationdb` exposes the Ecto.Repo transaction function, and it also exposes
  a FoundationDB-specific transaction.

  ```elixir
  TestRepo.transaction(fn ->
      # Ecto work
    end, prefix: tenant)
  ```

  ```elixir
  FoundationDB.transactional(tenant,
    fn tx ->
      # :erlfdb work
    end)
  ```

  Both of these calling convetions create a transaction on FDB. Typically you will use
  `Repo.transaction/1` when operating on your Ecto.Schema structs. If you wish
  to do anything using the lower-level `:erlfdb` API, you will use
  `FoundationDB.transactional/2`.

  Please visit the (FoundationDB Developer Guid on transactions)[https://apple.github.io/foundationdb/developer-guide.html#transaction-basics]
  for more information about how to develop with transactions.

  It's important to remember that even though the database gives ACID guarantees about the
  keys updated in a transaction, the Elixir function passed into `Repo.transaction/1` or
  `FoundationDB.transactional/2` will be executed more than once when any conflicts
  are encountered. For this reason, your function must not have any side effects other than
  the database operations. For example, do not publish any messages to other processes
  (such as `Phoenix.PubSub`) from within the transaction unless you are willing to receive
  early or duplicate messages.

  ```elixir
  # Do not do this!
  TestRepo.transaction(fn ->
    TestRepo.insert(%User{name: "John"})
    ...
    Phoenix.PubSub.broadcast("my_topic", "new_user") # Not safe! :(
  end, prefix: tenant)

  # Instead, do this:
  TestRepo.transaction(fn ->
    TestRepo.insert(%User{name: "John"})
    ...
  end, prefix: tenant)
  Phoenix.PubSub.broadcast("my_topic", "new_user") # Safe :)
  ```

  ### Migrations

  At first glance, `:ecto_foundationdb` migrations may look similar to that of `:ecto_sql`,
  but the actual execution of migrations and how your app must be configured are very
  different, so please read this section in full.

  If your app uses indexes on any of your schemas, you must define a `:migrator`
  option on your repo that is a module implementing the `Ecto.Adapters.FoundationDB.Migrator`
  behaviour.

  Migrations are only used for index management. There are no tables to add, drop, or modify.
  When you add a field to your Schema, any read requests on old objects will return `nil` in
  that field. Right now there is no way to rename a field.

  As tenants are opened during your application runtime, migrations will be executed
  automatically. This distributes the migration across a potentially long period of time,
  as migrations will not be executed unless the tenant is opened.

  Migrations can be completed in full with a call to
  `Ecto.Adapters.FoundationDB.Migrator.up_all/1`. Depending on how many tenants you have in
  your database, and the size of the data for each tenant, this operation might take a long
  time and make heavy use of system resources. It is safe to interrupt this operation, as
  migrations are performed transactionally. However, if you interrupt a migration, the next
  attempt for that tenant may have a brief delay (~5 sec); the migrator must ensure that the
  previous migration has indeed stopped executing.

  The `:migrator` is a module in your application runtime that provides the full list of
  ordered migrations. These are the migrations that will be executed when a tenant is opened.
  If you leave out a migration from the list, it will not be applied.

  For example, your migrator might look like this:

  ```elixir
  defmodule MyApp.Migrator do
    @behaviour Ecto.Adapters.FoundationDB.Migrator

    @impl true
    def migrations(MyApp.Repo) do
      [
        {0, MyApp.AMigrationForIndexCreation},
        {1, MyApp.AnIndexWeAddedLaterOn}
      ]
    end
  end
  ```

  As each tenant is opened at runtime, it will advance version-by-version in
  FDB transactions until it reaches the latest version.

  Each migration is contained in a separate module, much like EctoSQL's. However, the operations
  to be carried out **must be returned as a list.** For example, the creation of 2 indexes
  may look like this:

  ```elixir
  defmodule MyApp.AMigrationForIndexCreation do
    use Ecto.Adapters.FoundationDB.Migration
    def change() do
      [
        create(index(User, [:name]),
        create(index(Post, [:user_id]))
      ]
    end
  end
  ```

  Note: The following are yet to be implemented.

  1. Dropping an index
  2. Moving down in migration versions (i.e. rollback)

  Finally, the Mix tasks regarding ecto migrations are not supported.

  ```elixir
  # These commands are not supported. Do not use them with :ecto_foundationdb!
  #    mix ecto.migrate
  #    mix ecto.gen.migration
  #    mix ecto.rollback
  #    mix ecto.migrations
  ```

  ### Other Ecto Features

  Many of Ecto's features probably do not work with `ecto_foundationdb`. Please
  see the integration tests for a collection of use cases that is known to work.

  Certainly, you'll find that most queries fail with an Ecto.Adapaters.FoundationDB.Exception.Unsupported
  exception. The FoundationDB Layer concept precludes complex queries from being executed
  within the database. Therefore, it only makes sense to implement a limited set of query types --
  specifically those that do have optimized database query semantics. All other filtering, aggregation,
  and grouping must be done by your Elixir code.
  """

  @behaviour Ecto.Adapter
  @behaviour Ecto.Adapter.Storage
  @behaviour Ecto.Adapter.Schema
  @behaviour Ecto.Adapter.Queryable
  @behaviour Ecto.Adapter.Transaction

  alias Ecto.Adapters.FoundationDB.Database
  alias Ecto.Adapters.FoundationDB.EctoAdapter
  alias Ecto.Adapters.FoundationDB.EctoAdapterQueryable
  alias Ecto.Adapters.FoundationDB.EctoAdapterSchema
  alias Ecto.Adapters.FoundationDB.EctoAdapterStorage
  alias Ecto.Adapters.FoundationDB.EctoAdapterTransaction
  alias Ecto.Adapters.FoundationDB.Layer.Tx
  alias Ecto.Adapters.FoundationDB.Options
  alias Ecto.Adapters.FoundationDB.Tenant

  @spec db(Ecto.Repo.t()) :: Database.t()
  def db(repo) when is_atom(repo) do
    db(repo.config())
  end

  @spec db(Options.t()) :: Database.t()
  def db(options) do
    case :persistent_term.get({__MODULE__, :database}, nil) do
      nil ->
        db = EctoAdapterStorage.open_db(options)
        :persistent_term.put({__MODULE__, :database}, {db, options})
        db

      {db, _options} ->
        db
    end
  end

  @spec usetenant(Ecto.Schema.schema(), any()) :: Ecto.Schema.schema()
  def usetenant(struct, tenant) do
    Ecto.put_meta(struct, prefix: tenant)
  end

  @doc """
  Executes the given function in a transaction on the database.

  If you provide an arity-0 function, your function will be executed in
  a newly spawned process. This is to ensure that EctoFoundationDB can
  safely manage the process dictionary.

  Please be aware of the
  [limitations that FoundationDB](https://apple.github.io/foundationdb/developer-guide.html#transaction-basics)
  imposes on transactions.

  For example, a transaction must complete
  [within 5 seconds](https://apple.github.io/foundationdb/developer-guide.html#long-running-transactions).
  """
  @spec transactional(Database.t() | Tenant.t() | nil, function()) :: any()
  def transactional(db_or_tenant, fun), do: Tx.transactional_external(db_or_tenant, fun)

  @impl Ecto.Adapter
  defmacro __before_compile__(_env), do: :ok

  @impl Ecto.Adapter
  defdelegate ensure_all_started(config, type), to: EctoAdapter

  @impl Ecto.Adapter
  defdelegate init(config), to: EctoAdapter

  @impl Ecto.Adapter
  defdelegate checkout(data, config, fun), to: EctoAdapter

  @impl Ecto.Adapter
  defdelegate checked_out?(data), to: EctoAdapter

  @impl Ecto.Adapter
  defdelegate loaders(primitive_type, ecto_type), to: EctoAdapter

  @impl Ecto.Adapter
  defdelegate dumpers(primitive_type, ecto_type), to: EctoAdapter

  @impl Ecto.Adapter.Storage
  defdelegate storage_up(options), to: EctoAdapterStorage

  @impl Ecto.Adapter.Storage
  defdelegate storage_down(options), to: EctoAdapterStorage

  @impl Ecto.Adapter.Storage
  defdelegate storage_status(options), to: EctoAdapterStorage

  @impl Ecto.Adapter.Schema
  defdelegate autogenerate(type), to: EctoAdapterSchema

  @impl Ecto.Adapter.Schema
  defdelegate insert_all(
                adapter_meta,
                schema_meta,
                header,
                unsure,
                on_conflict,
                returning,
                placeholders,
                options
              ),
              to: EctoAdapterSchema

  @impl Ecto.Adapter.Schema
  defdelegate insert(adapter_meta, schema_meta, fields, on_conflict, returning, options),
    to: EctoAdapterSchema

  @impl Ecto.Adapter.Schema
  defdelegate update(adapter_meta, schema_meta, fields, filters, returning, options),
    to: EctoAdapterSchema

  @impl Ecto.Adapter.Schema
  defdelegate delete(adapter_meta, schema_meta, filters, returning, options),
    to: EctoAdapterSchema

  @impl Ecto.Adapter.Queryable
  defdelegate prepare(atom, query), to: EctoAdapterQueryable

  @impl Ecto.Adapter.Queryable
  defdelegate execute(adapter_meta, query_meta, query_cache, params, options),
    to: EctoAdapterQueryable

  @impl Ecto.Adapter.Queryable
  defdelegate stream(adapter_meta, query_meta, query_cache, params, options),
    to: EctoAdapterQueryable

  @impl Ecto.Adapter.Transaction
  defdelegate transaction(adapter_meta, options, function), to: EctoAdapterTransaction

  @impl Ecto.Adapter.Transaction
  defdelegate in_transaction?(adapter_meta), to: EctoAdapterTransaction

  @impl Ecto.Adapter.Transaction
  defdelegate rollback(adapter_meta, value), to: EctoAdapterTransaction
end
