defmodule Ecto.Adapters.FoundationDB do
  @moduledoc """

  ([Hex.pm](https://hex.pm/packages/ecto_foundationdb) | [GitHub](https://github.com/foundationdb-beam/ecto_foundationdb))

  Adapter module for FoundationDB.

  It uses `:erlfdb` for communicating with the database.

  ## Installation

  Install the latest stable release of FoundationDB from the
  [official FoundationDB Releases](https://github.com/apple/foundationdb/releases).

  You will only need to install the `foundationdb-server` package if you're
  running an instance of the FoundationDB Server. For example, it's common to
  run the `foundationdb-server` on your development machine and on managed
  instances running a FoundationDB cluster, but not for your stateless Elixir
  application server in production.

  `foundationdb-clients` is always required.

  Include `:ecto_foundationdb` in your list of dependencies in `mix.exs`:

  ```elixir
  defp deps do
    [
      {:ecto_foundationdb, "~> 0.1.0"}
    ]
  end
  ```

  ## Usage

  Define this module in your `MyApp` application:

  ```elixir
  defmodule MyApp.Repo do
    use Ecto.Repo, otp_app: :my_app, adapter: Ecto.Adapters.FoundationDB

    use EctoFoundationDB.Migrator
    def migrations(), do: []
  end
  ```

  Edit your `config.exs` file to include the following:

  ```elixir
  config :my_app,
    ecto_repos: [MyApp.Repo]

  config :my_app, MyApp.Repo,
    cluster_file: "/etc/foundationdb/fdb.cluster"
  ```

  ## Tenants

  EctoFoundationDB requires the use of [FoundationDB Tenants](https://apple.github.io/foundationdb/tenants.html).

  Each tenant you create has a separate keyspace from all others, and a given FoundationDB
  Transaction is guaranteed to be isolated to a particular tenant's keyspace.

  You'll use the Ecto `:prefix` option to specify the relevant tenant for each Ecto operation
  in your application.

  Creating a schema to be used in a tenant.

  ```elixir
  defmodule User do
    use Ecto.Schema
    @schema_context usetenant: true
    @primary_key {:id, :binary_id, autogenerate: true}
    schema "users" do
      field(:name, :string)
      field(:department, :string)
      timestamps()
    end
    # ...
  end
  ```

  Setting up a tenant.

  ```elixir
  alias EctoFoundationDB.Tenant

  tenant = Tenant.open!(MyApp.Repo, "some-org")
  ```

  Inserting a new struct.

  ```elixir
  user = %User{name: "John", department: "Engineering"}
         |> FoundationDB.usetenant(tenant)

  user = MyApp.Repo.insert!(user)
  ```

  Querying for a struct using the primary key.

  ```elixir
  MyApp.Repo.get!(User, user.id, prefix: tenant)
  ```

  When a struct is retrieved from a tenant (using `:prefix`), that struct's metadata
  holds onto the tenant reference. This helps to protect your application from a
  struct accidentally crossing tenant boundaries due to some unforeseen bug.

  ## Indexes

  The implication of the [FoundationDB Layer Concept](https://apple.github.io/foundationdb/layer-concept.html)
  is that the manner in which data can be stored and accessed is the responsibility of the client
  application. To achieve general purpose data storage and access patterns commonly desired
  in web applications, EctoFoundationDB provides support for indexes out of the box, and
  also allows your application to define custom indexes.

  When you define a migration that calls `create index(User, [:department])`, a `Default` index is
  created on the `:department` field. The following two indexes are equivalent:

  ```elixir
  # ...
  create index(User, [:department])
  # is equivalent to
  create index(User, [:department], options: [indexer: EctoFoundationDB.Indexer.Default]))]
  # ...
  ```

  Timestamp indexes and multi-field indexes are supported:

  ```elixir
  create index(User, [:inserted_at])
  create index(User, [:birthyear, :department, :birthdate])
  ```

  In fact, the index value can be any Elixir term. All types support Equal queries. However,
  certain Ecto Schema types support Between queries. See the Data Types section for the list.

  For example, When an index is created on timestamp-like fields, it is an effective time
  series index. See the query examples below.

  See the Migrations section for further details about managing indexes in your application.

  ### Index Query Examples

  Retrieve all users in the Engineering department (with an Equal constraint):

  ```elixir
  iex> query = from(u in User, where: u.department == ^"Engineering")
  iex> MyApp.Repo.all(query, prefix: tenant)
  ```

  Retrieve all users with a birthyear from 1992 to 1994 (with a Between constraint):

  ```elixir
  iex> query = from(u in User,
  ...>   where: u.birthyear >= ^1992 and u.birthyear <= ^1994
  ...> )
  iex> MyApp.Repo.all(query, prefix: tenant)
  ```

  Retrieve all Engineers born in August 1992:

  ```elixir
  iex> query = from(u in User,
  ...>   where: u.birthyear == ^1992 and
  ...>          u.department == ^"Engineering" and
  ...>          u.birthdate >= ^~D[1992-08-01] and u.birthdate < ^~D[1992-09-01]
  ...> )
  iex> MyApp.Repo.all(query, prefix: tenant)
  ```

  **Order matters!**: When you create an index using multiple fields, the FDB key that stores the index will be extended with
  all the values in the order of your defined index fields. Because FoundationDB stores keys in a well-defined order,
  the order of the fields in your index determines the Between queries you can perform.

  There can be 0 or 1 Between clauses, and if one exists, it must correspond to the final constraint in the where clause when
  compared against the order of the index fields.

  ### Custom Indexes

  As data is inserted, updated, deleted, and queried, the Indexer callbacks (via behaviour `EctoFoundationDB.Indexer`)
  are executed. Your Indexer module does all the work necessary to maintain the index within the transaction
  that the data itself is being manipulated. Many different types of indexes are possible. You are in control!

  For an example, please see the [MaxValue implementation](https://github.com/foundationdb-beam/ecto_foundationdb/blob/main/lib/ecto_foundationdb/indexer/max_value.ex)
  which is used internally by EctoFoundationDB to keep track of the maximum value of a field in a Schema.

  ## Transactions

  The Repo functions will automatically use FoundationDB transactions behind the scenes. EctoFoundationDB also
  exposes the Ecto.Repo transaction function, allowing you to group operations together with transactional isolation:

  ```elixir
  MyApp.Repo.transaction(fn ->
      # Ecto work
    end, prefix: tenant)
  ```

  It also exposes a FoundationDB-specific transaction:

  ```elixir
  alias Ecto.Adapters.FoundationDB

  FoundationDB.transactional(tenant,
    fn tx ->
      # :erlfdb work
    end)
  ```

  Both of these calling conventions create a transaction on FDB. Typically you will use
  `Repo.transaction/1` when operating on your Ecto.Schema structs. If you wish
  to do anything using the lower-level `:erlfdb` API (rare), you will use
  `FoundationDB.transactional/2`.

  Please read the [FoundationDB Developer Guid on transactions](https://apple.github.io/foundationdb/developer-guide.html#transaction-basics)
  for more information about how to develop with transactions.

  It's important to remember that even though the database gives ACID guarantees about the
  keys updated in a transaction, the Elixir function passed into `Repo.transaction/1` or
  `FoundationDB.transactional/2` will be executed more than once when any conflicts
  are encountered. For this reason, your function must not have any side effects other than
  the database operations. For example, do not publish any messages to other processes
  (such as `Phoenix.PubSub`) from within the transaction.

  ```elixir
  # Do not do this!
  MyApp.Repo.transaction(fn ->
    MyApp.Repo.insert(%User{name: "John"})
    ...
    Phoenix.PubSub.broadcast("my_topic", "new_user") # Not safe! :(
  end, prefix: tenant)
  ```

  ```elixir
  # Instead, do this:
  MyApp.Repo.transaction(fn ->
    MyApp.Repo.insert(%User{name: "John"})
    ...
  end, prefix: tenant)
  Phoenix.PubSub.broadcast("my_topic", "new_user") # Safe :)
  ```

  ## Migrations

  At first glance, EctoFoundationDB migrations may look similar to that of `:ecto_sql`,
  but the actual execution of migrations and how your app must be configured are very
  different, so please read this section in full.

  If your app uses indexes on any of your schemas, you must include a module that implements
  the `EctoFoundationDB.Migrator` behaviour. By default, this is assumed to be your Repo
  module, as we have demonstrated above.

  Migrations are only used for index management. There are no tables to add, drop, or modify.
  When you add a field to your Schema, any read requests on old objects will return `nil` in
  that field.

  As tenants are opened during your application runtime, migrations will be executed
  automatically. This distributes the migration across a potentially long period of time,
  as migrations will not be executed unless the tenant is opened.

  The Migrator is a module in your application runtime that provides the full list of
  ordered migrations. These are the migrations that will be executed when a tenant is opened.
  If you leave out a migration from the list, it will not be applied.

  The result of `migrations/0` will change over time to include new migrations, like so:

  ```elixir
  defmodule MyApp.Repo do
    # ...
    def migrations() do
      [
        {0, MyApp.IndexUserByDepartment}
        # At a later date, we my add a new index with a corresponding addition
        # tho the migrations list:
        #    {1, MyApp.IndexUserByRole}
      ]
    end
  end
  ```

  Each migration is contained in a separate module, much like EctoSQL's. However, the operations
  to be carried out **must be returned as a list.** For example, the creation an index
  may look like this:

  ```elixir
  defmodule MyApp.IndexUserByDepartment do
    use EctoFoundationDB.Migration
    def change() do
      [
        create index(User, [:department])
      ]
    end
  end
  ```

  Your migrator and `MyApp.<Migration>` modules are part of your codebase. They must be
  included in your application release.

  Migrations can be completed in full at any time with a call to
  `EctoFoundationDB.Migrator.up_all/1`. Depending on how many tenants you have in
  your database, and the size of the data for each tenant, this operation might take a long
  time and make heavy use of system resources. It is safe to interrupt this operation, as
  migrations are performed transactionally. However, if you interrupt a migration, the next
  attempt for that tenant may have a brief delay (~5 sec); the migrator must ensure that the
  previous migration has indeed stopped executing.

  ## Data Types

  EctoFoundationDB stores your struct's data using `:erlang.term_to_binary/1`, and
  retrieves it with `:erlang.binary_to_term/1`. As such, there is no data type
  conversion between Elixir types and Database Types. Any term you put in your
  struct will be stored and later retrieved.

  Data types are used by EctoFoundationDB for the creation and querying of indexes.
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

  ## Standard Options

    * `:cluster_file` - The path to the fdb.cluster file. The default is
      `"/etc/foundationdb/fdb.cluster"`.
    * `:migrator` - A module that implements the `EctoFoundationDB.Migrator`
      behaviour. Required when using any indexes. Defaults to `nil`. When `nil`,
      your Repo module is assumed to be the Migrator.

  ## Advanced Options

    * `:storage_id` - All tenants created by this adapter are prefixed with
      this string. This allows multiple configurations to operate on the
      same FoundationDB cluster indepedently. Defaults to
      `"Ecto.Adapters.FoundationDB"`.
    * `:open_db` - 0-arity function used for opening a reference to the
       FoundationDB cluster. Defaults to `:erlfdb.open(cluster_file)`. When
       using `EctoFoundationDB.Sandbox`, you should consider setting
       this option to `Sandbox.open_db/0`.
    * `:migration_step` - The maximum number of keys to process in a single transaction
      when running migrations. Defaults to `1000`. If you use a number that is
      too large, the FDB transactions run by the Migrator will fail.
    * `:idx_cache` - When set to `:enabled`, the Ecto ets cache is used to store the
      available indexes per tenant. This speeds up all database operations.
      Defaults to `:enabled`.

  ## Limitations and Caveats

  ### FoundationDB

  Please read [FoundationDB Known Limitations](https://apple.github.io/foundationdb/known-limitations.html).

  ### Layer

  EctoFoundationDB implements a specific Layer on the FoundationDB
  key-value store. This Layer is intended to be generally useful, but you
  may not find it suitable for your workloads. The Layer is documented in
  detail at `EctoFoundationDB.Layer`. This project does not support
  plugging in other Layers.

  ### Tenants

   * The use of a tenant implies an ownership relationship between the tenant and the data.
     It's up to you how you use this relationship in your application.

   * Failure to specify a tenant will result in a raised exception at runtime.

  ### Migrations

  The following are not supported:

  * Renaming a table
  * Renaming a field
  * Dropping an index
  * Moving down in migration bersions (rollback)

  Finally, the Ecto Mix tasks are known to be unsupported:

  ```elixir
  # These commands are not supported. Do not use them with :ecto_foundationdb!
  #    mix ecto.create
  #    mix ecto.drop
  #    mix ecto.migrate
  #    mix ecto.gen.migration
  #    mix ecto.rollback
  #    mix ecto.migrations
  ```

  ### Key and Value Size

  [FoundationDB has limits on key and value size](https://apple.github.io/foundationdb/known-limitations.html#large-keys-and-values)
  Please be aware of these limitations as you develop. EctoFoundationDB doesn't make
  any attempt to protect you from these errors.

  ### Ecto Queries

  You'll find that most queries outside of those detailed in the documentation fail with
  an `EctoFoundationDB.Exception.Unsupported` exception. The FoundationDB Layer concept
  precludes complex queries from being executed within the database. Therefore, it only makes sense
  to implement a limited set of query types -- specifically those that do have optimized database
  query semantics. All other filtering, aggregation, and grouping must be done by your Elixir code.

  In other words, you'll be writing less SQL, and more Elixir.

  Refer to the integration tests for a collection of queries that is known to work.

  ### Other Ecto Features

  As you test the boundaries of EctoFoundationDB, you'll find that many other Ecto features just plain
  don't work with this adapter. Ecto has a lot of features, and this adapter is in the early stage.
  For any given unsupported feature, the underlying reason will be one of the following.

  1. It makes sense for EctoFoundationDB, but it just hasn't been implemented yet.
  2. The fundamental differences between FDB and SQL backends mean that there is
     no practical reason to implement it in EctoFoundationDB.

  Please feel free to request features, but also please be aware that your request
  might be closed if it falls into that second category.
  """

  @behaviour Ecto.Adapter
  @behaviour Ecto.Adapter.Storage
  @behaviour Ecto.Adapter.Schema
  @behaviour Ecto.Adapter.Queryable
  @behaviour Ecto.Adapter.Transaction

  alias EctoFoundationDB.Database
  alias EctoFoundationDB.Layer.Tx
  alias EctoFoundationDB.Options
  alias EctoFoundationDB.Tenant

  alias Ecto.Adapters.FoundationDB.EctoAdapter
  alias Ecto.Adapters.FoundationDB.EctoAdapterQueryable
  alias Ecto.Adapters.FoundationDB.EctoAdapterSchema
  alias Ecto.Adapters.FoundationDB.EctoAdapterStorage
  alias Ecto.Adapters.FoundationDB.EctoAdapterTransaction

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
