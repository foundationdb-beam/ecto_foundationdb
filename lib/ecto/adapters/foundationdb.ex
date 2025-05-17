defmodule Ecto.Adapters.FoundationDB do
  @moduledoc """

  ([Hex.pm](https://hex.pm/packages/ecto_foundationdb) | [GitHub](https://github.com/foundationdb-beam/ecto_foundationdb))

  Adapter module for [FoundationDB](https://www.foundationdb.org).

  It uses [erlfdb](https://github.com/foundationdb-beam/erlfdb) for communicating with the database,
  and implements an Ecto-compatible Layer on top of FDB's key-value store.

  ## Installation

  Install the latest stable release of FoundationDB from the
  [official FoundationDB Releases](https://github.com/apple/foundationdb/releases).

  The `foundationdb-server` package is required on any system that will be running
  a FoundationDB server instance. For example, it's common to
  run the `foundationdb-server` on your development machine and on managed
  instances running a FoundationDB cluster, but not for your stateless Elixir
  application server in production.

  `foundationdb-clients` is always required.

  Include `:ecto_foundationdb` in your list of dependencies in `mix.exs`:

  ```elixir
  defp deps do
    [
      {:ecto_foundationdb, "~> 0.4"}
    ]
  end
  ```

  ## Usage

  Define this module in your `MyApp` application:

  ```elixir
  defmodule MyApp.Repo do
    use Ecto.Repo, otp_app: :my_app, adapter: Ecto.Adapters.FoundationDB

    use EctoFoundationDB.Migrator

    @impl true
    def migrations(), do: []
  end
  ```

  Edit your `config.exs` file to include the following:

  ```elixir
  config :my_app, MyApp.Repo,
    cluster_file: "/etc/foundationdb/fdb.cluster"
  ```

  In your app's supervisor, you can start the repo as follows:

  ```elixir
  children = [
    ...
    MyApp.Repo,
    ...
  ]

  Supervisor.start_link(...)
  ```

  ### Tenants

  EctoFoundationDB requires your application to use multitenancy via `EctoFoundationDB.Tenant`.
  We strongly encourage your application to decide on a multitenancy strategy that makes sense
  for your problem domain (or simply use `"default"` to get started).

  Once you have opened a tenant, you'll use the Ecto `:prefix` option to provide the tenant
  to each Repo operation.

  Retrieving data from a tenant yields no execution overhead. In effect, each tenant can be
  viewed as a logically partitioned database. See `EctoFoundationDB.Tenant` for a discussion
  on the mechanism used to achieve this in FDB.

  ### Creating a schema

  This is a standard `Ecto.Schema` that will be used in the examples of this documentation.

  ```elixir
  defmodule User do
    use Ecto.Schema
    @primary_key {:id, :binary_id, autogenerate: true}
    schema "users" do
      field(:name, :string)
      field(:department, :string)
      field(:balance, :integer)
      timestamps()
    end
    # ...
  end
  ```

  ### Opening a tenant

  See `EctoFoundationDB.Tenant` for more details.

  ```elixir
  alias EctoFoundationDB.Tenant

  tenant = Tenant.open!(MyApp.Repo, "some-org")
  ```

  ### Inserting a new struct

  The Tenant is stored on the struct as metadata, so that when passing structs
  to Repo functions, the `:prefix` option is not required.

  ```elixir
  alice = %User{name: "Alice", department: "Engineering"}
         |> FoundationDB.usetenant(tenant)

  alice = MyApp.Repo.insert!(alice)
  ```

  ### Querying for a struct using the primary key

  The `:prefix` option is required. The struct returned will have the tenant
  in the metadata, like above.

  ```elixir
  MyApp.Repo.get!(User, alice.id, prefix: tenant)
  ```

  ### Transaction basics

  Here's a simple example of transaction usage. Notice that the calls to Repo inside the transaction function
  do not include the `:prefix` option. See [Transactions](#module-transactions) for more information.

  ```elixir
  MyApp.Repo.transaction(fn ->
    alice = MyApp.Repo.get!(User, alice.id)
    if alice.balance > 0 do
      MyApp.Repo.update(User.changeset(alice, %{balance: alice.balance - 1}))
    else
      raise "Overdraft"
    end
  end, prefix: tenant)
  ```

  ## Indexes and queries

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

  In fact, the index value can be any Elixir term. All types support Equal queries (of the form `x.field == "value"`).
  Certain Ecto Schema types support Between queries (of the form `x.field >= 10 and x.field < 20`).
  See the [Data Types](#module-data-types) section for the list.

  See the [Migrations](#module-migrations) section for further details about managing indexes in your application.

  ### Index query examples

  Retrieve all users in the Engineering department (with an Equal constraint):

  ```elixir
  query = from(u in User, where: u.department == ^"Engineering")
  MyApp.Repo.all(query, prefix: tenant)
  ```

  Retrieve all users with a birthyear from 1992 to 1994 (with a Between constraint):

  ```elixir
  query = from(u in User,
    where: u.birthyear >= ^1992 and u.birthyear <= ^1994
  )
  MyApp.Repo.all(query, prefix: tenant)
  ```

  Retrieve all Engineers born in August 1992:

  ```elixir
  query = from(u in User,
    where: u.birthyear == ^1992 and
           u.department == ^"Engineering" and
           u.birthdate >= ^~D[1992-08-01] and u.birthdate < ^~D[1992-09-01]
  )
  MyApp.Repo.all(query, prefix: tenant)
  ```

  ### Index ordering matters

  When you create an index using multiple fields, the FDB key that stores the index will be extended with
  all the values *in the order of your defined index fields*. Because FoundationDB stores the keys themselves
  in a well-defined order, the order of the fields in your index determines the Between queries you can perform.

  > **_RULE:_**  For any Query, there can be 0 or 1 Between clauses, and if one exists for a field, then there must not
  exist an Equal clause for ay field that follows, according to the list of fields in the index. Moreover, the Between
  field must be of a [Data Type](#module-data-types) supported for Between queries.

  Above, we demonstrated a nontrivial query with 2 Equal constraints and 1 Between constraint. Compare that with an invalid
  query below, which attempts a Between constraint on the `:birthyear` field followed by an Equal constraint on the `:department`
  field.

  ```elixir
  # Invalid query!
  from(u in User,
    where: u.birthyear >= ^1992 and u.birthyear < ^1995 and
           u.department == ^"Engineering"
  )
  ```

  **Why is this query unsupported by EctoFDB?** With the index defined as-is, EctoFDB is unable to extract a minimal
  dataset from the database in one operation. Instead of over-extracting data and then applying a filter, we've chosen
  to reject the query. We hope this encourages correct index management and gives the developer confidence about the
  expected execution time for any given call to Repo.

  **Yeah that's cool, but how do I run the query I want to run?** You have 2 options.

  1. Purposefully over-extract the data using Stream, and do the filtering in Elixir:

  ```elixir
  from(u in User, where: u.birthyear >= ^1992 and u.birthyear < 1995 and)
  |> MyApp.Repo.stream(prefix: tenant)
  |> Stream.filter(&((&1).department == "Engineering"))
  |> Enum.to_list()
  ```

  or

  2. Create the necessary index. For our example above, the new index might look like:

  ```elixir
  create index(User, [:department, :birthyear])
  ```

  We encourage you to test execution times of your expected workload before deciding to create an index.
  Remember: since your data is already partitioned into tenants, you're effectively getting 1 index for free.

  ### Custom indexes

  As data is inserted, updated, deleted, and queried, the Indexer callbacks (via behaviour `EctoFoundationDB.Indexer`)
  are executed. Your Indexer module does all the work necessary to maintain the index within the transaction
  that the data itself is being manipulated. Many different types of indexes are possible. You are in control!

  For an example, please see the [Default implementation](https://github.com/foundationdb-beam/ecto_foundationdb/blob/main/lib/ecto_foundationdb/indexer/default.ex).

  ## Transactions

  All work involving FDB happens in a transaction, including each Repo function call (e.g. `Repo.get/1`). EctoFoundationDB also
  exposes the `Repo.transaction/2` function, allowing you to group operations together with transactional isolation ([ACID](https://en.wikipedia.org/wiki/ACID)
  and [globally serializable](https://en.wikipedia.org/wiki/Global_serializability))

  ```elixir
  MyApp.Repo.transaction(fn ->
      # Ecto work
    end, prefix: tenant)
  ```

  Please read the [FoundationDB Developer Guide on transactions](https://apple.github.io/foundationdb/developer-guide.html#transaction-basics)
  for more information about how to develop with transactions.

  It's important to remember that even though the database gives ACID guarantees about the
  keys updated in a transaction, the Elixir function passed into `Repo.transaction/2`
  may be executed more than once when any conflicts
  are encountered. For this reason, your function must not have any side effects other than
  the database operations. For example, do not publish any messages to other processes
  (such as `Phoenix.PubSub`) from within the transaction.

  ```elixir
  # Do not do this!
  MyApp.Repo.transaction(fn ->
    MyApp.Repo.insert(%User{name: "Alice"})
    ...
    Phoenix.PubSub.broadcast("my_topic", "new_user") # Not safe! :(
  end, prefix: tenant)
  ```

  ```elixir
  # Instead, do this:
  MyApp.Repo.transaction(fn ->
    MyApp.Repo.insert(%User{name: "Alice"})
    ...
  end, prefix: tenant)
  Phoenix.PubSub.broadcast("my_topic", "new_user") # Safe :)
  ```

  Note: If you're looking for PubSub-like functionality pushed from the database itself, please see
  [Watches](#module-watches).

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

  ### Online migrations

  As tenants are opened during your application runtime, migrations will be executed
  automatically in a "lazy" fashion. This distributes the migration across a potentially long period of time,
  as migrations will not be executed unless the tenant is opened. Specifically, a call to
  `Tenant.open/2` will block until the migration for that tenant completes. Other tenants
  will not be affected.

  For example, imagine you have 2 nodes running your application. Node 1 has opened tenants `"a"`
  and `"b"`. You deploy a new version of your app to Node 2 that includes a new index, and open
  tenant `"a"` there. On Node 2 only, that call to `Tenant.open/2` will block until the migration
  completes. All operations on Node 1 will remain working. When `Tenant.open/2` completes on Node 2
  for tenant `"a"`, the index is available to all `"a"` tenants across all nodes. Any `"b"` tenants
  remain unaffected throughout this process.

  ### Configuring your app

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

    @impl true
    def change() do
      [
        create index(User, [:department])
      ]
    end
  end
  ```

  Your migrator and `MyApp.<Migration>` modules are part of your codebase. They must be
  included in your application release. This differs from traditional Ecto migrations (`:ecto_sql`), which
  are typically managed in `.exs` scripts executed outside the production application runtime.

  ### Creating and dropping indexes

  In your Migration, to create an index use `create index(table, columns)` and to drop an index use `drop index(table, columns)`.

  Indexes are created and dropped in a manner that is safe for your application transactions.

  ```elixir
  defmodule MyApp.ExampleMigration do
    use EctoFoundationDB.Migration

    @impl true
    def change() do
      [
        create index(User, [:role]),
        drop index(User, [:department]),
      ]
    end
  end
  ```

  ### Executing all migrations immediately

  Migrations can be completed in full at any time with a call to
  `EctoFoundationDB.CLI.migrate!/1`, which accepts your repo module (e.g. `MyApp.Repo`).

  ```elixir
  # May take a while to complete
  EctoFoundationDB.CLI.migrate!(MyApp.Repo)
  ```

  Depending on how many tenants you have in your database, and the size of the data for each tenant,
  this operation might take a long time and make heavy use of system resources. It is safe to interrupt
  this operation, as migrations are performed transactionally. However, if you interrupt a migration, the next
  attempt for that tenant may have a brief delay (~5 sec) while the migrator ensures that the
  previous migration has indeed stopped executing.

  Please refer to the documentation of `EctoFoundationDB.CLI` for other actions to be carried out
  by a human operator.

  ## Data types

  EctoFoundationDB stores your struct's data using `:erlang.term_to_binary/1`, and
  retrieves it with `:erlang.binary_to_term/1`. As such, there is no data type
  conversion between Elixir types and Database Types. Any term you put in your
  struct will be stored and later retrieved.

  Data types *are* used by EctoFoundationDB for the creation and querying of indexes.
  Certain Ecto types are encoded into the FDB key, which allows you to formulate
  Between queries on indexes of these types.

  ** Between queries are supported for these types:**

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

  See [Indexes and Queries](#module-indexes-and-queries) for more information.

  ## Watches

  [FoundationDB Watches](https://apple.github.io/foundationdb/developer-guide.html#watches) are
  similar to Triggers in an RDBMS. Registering a watch on a particular key provides a guarantee[*](https://github.com/apple/foundationdb/wiki/An-Overview-how-Watches-Work)
  that when that key in the database is changed, the client application is notified with a
  push-style notification. This registration is initiated inside a transaction, and the caller is
  provided a `EctoFoundationDB.Future` object that is used to receive the push notification at a later time.
  The notification is delivered directly to the Elixir process that requested it.

  The EctoFDB Repo module allows you to register a watch on any struct that you have written
  to the database. The FDB watch is registered on the key that represents the Primary Write (the key-value
  that stores all the data of your struct). When that struct changes in the database via any means,
  the Future is resolved and the caller has the opportunity to refresh its internal state.

  ### EctoFDB Repo functions for watches

  EctoFDB adds the following functions to your Repo module. They are each discussed in detail
  below.

  - `Repo.watch/1`/`Repo.watch/2`: Registers a watch
  - `Repo.assign_ready/2`/`Repo.assign_ready/3`: Handles Future resolution via a push message

  ### `Repo.watch/2`

  With arguments `struct` and `options`, registers
  a watch on the given struct and returns a Future. This Future resolves to
  a value when the FDB key is set or cleared. Until then, the Future remains unresolved.
  The Future from a watch is resolvable outside of an FDB transaction.

  In addition to other standard options, a `:label` option will prime the Future for use by `assign_ready/3`.
  It's recommended that you provide a label and use `assign_ready/3`, but not required.

  ### `Repo.assign_ready/3`

  With arguments `futures`, `ready_refs`, and
  `options`, returns a tuple containing: 1) a Keyword of structs with keys according to the `:label`
  provided to `watch/2`, and 2) an updated list of futures that are still unresolved.

  In order for the caller to acquire the `ready_refs` arguments, you must receive messages from the
  process mailbox of the form `{ref, :ready}`. See the example below for detail. If your process chooses
  to buffer a list `ready_refs` longer than 1, then `assign_ready/3` uses pipelining to retrieve the
  resulting structs. Each watched struct must have been registered with a unique `:label`.

  The `options` are provided to `Repo.async_get/3` internally. In addition to standard options, a
  `watch?: true` option will re-register a new watch on the struct so that your calling
  process's event loop can always be aware of changes.

  ### Example

  The power of watches is best illustrated in the context of a stateful Elixir process that is
  interested in tracking the up-to-date value for a particular schema. This could be a GenServer, LiveView,
  or some other stateful process.

  ```elixir
  defmodule MyAliceView do

    # ...

    def init(tenant) do
      {alice, future} = MyRepo.transaction(
                          fn ->
                            alice = MyRepo.get_by(User, name: "Alice")
                            {alice, MyRepo.watch(alice, label: :alice)}
                          end, prefix: tenant)
      assigns = [alice: alice]
      {:ok, %{assigns: assigns, futures: [future]}}
    end

    # ...

    def handle_info({ref, :ready}, state=%{assigns: assigns, futures: futures, tenant: tenant}) when is_reference(ref) do
      {new_assigns, new_futures} = MyRepo.assign_ready(futures, [ref], watch?: true, prefix: tenant)
      {:noreply, %{
            state |
              assigns: Map.merge(assigns, Enum.into(new_assigns, %{})),
              futures: new_futures
            }}
    end
  end
  ```

  By handling the Future message resolution event `{ref, :ready}` in the `handle_info/2`, we never need to
  call `Repo.await/1`. Instead, our process can do other work and react to the `:ready` event immediately
  when it's delivered.

  And `Repo.assign_ready/3` provides a conventional way to retrieve the result and register a new watch
  in one go, so that updates to Alice are not missed.

  ## Upserts

  The FoundationDB Adapter supports the following upsert approaches.

  ### Upserts with `on_conflict`

  The following choices for `on_conflict` are supported, and they work in the manner
  described by the official Ecto documentation.

  - `:raise`: The default.
  - `:nothing`
  - `:replace_all`
  - `{:replace_all_except, fields}`
  - `{:replace, fields}`

  ### Upserts with `conflict_target`

  If you provide `conflict_target: []`, the FoundationDB Adapter will make no effort to
  inspect the existence of objects in the database before insert. When an existing
  object is blindly overwritten, your indexes may become inconsistent, resulting in undefined
  behavior. You should only consider its use if one of the following is true:

  - (a) your schema has no indexes
  - (b) you know apriori that any indexed fields are unchanged at the time of upsert, or
  - (c) you know apirori that the primary key you're inserting doesn't already exist.

  For these use cases, this option may speed up the loading of initial data at scale.

  ## Pipelining

  Pipelining is the technique of sending multiple queries to a database at the same time, and
  then receiving the results at a later time. In doing so, you can often avoid waiting
  for multiple network round trips. EctoFDB uses a `async`/`await` syntax to
  implement pipelining.

  ### EctoFDB Repo functions for pipelining

  EctoFDB adds the following functions to your Repo module. You'll notice that there are `async_*`
  versions of all the Queryable functions on the standard `Ecto.Repo`. These have the same arguments
  as their counterparts, and they each return a single `EctoFoundationDB.Future`.

  - `Repo.async_get/2` / `Repo.async_get/3`
  - `Repo.async_get!/2` / `Repo.async_get!/3`
  - `Repo.async_get_by/2` / `Repo.async_get_by/3`
  - `Repo.async_get_by!/2` / `Repo.async_get_by!/3`
  - `Repo.async_one/1` / `Repo.async_one/2`
  - `Repo.async_one!/1` / `Repo.async_one!/2`
  - `Repo.async_all/1` / `Repo.async_all/2`
  - `Repo.await/1`: The results of the above functions are provided to `Repo.await/1` to
    resolve the Futures. It accepts a single Future or a list of Futures.

  ### Example

  Consider this example. Our transaction below has 2 network round trips in serial.
  The "Alice" User is retrieved, and then the "Bob" user is retrieved. Finally, the
  list is constructed and the transaction ends.

  ```elixir
  result = MyApp.Repo.transaction(fn ->
    [
      MyApp.Repo.get_by(User, name: "Alice"),
      MyApp.Repo.get_by(User, name: "Bob")
    ]
  end, prefix: tenant)

  [alice, bob] = result
  ```

  With pipelining, we can issue 2 `async_get_by` queries without waiting for their result,
  and then only when we need the data, we `await` the result. Notice that the list of futures
  is constructed, and then we `await` on them.

  ```elixir
  result = MyApp.Repo.transaction(fn ->
    futures = [
      MyApp.Repo.async_get_by(User, name: "Alice"),
      MyApp.Repo.async_get_by(User, name: "Bob")
    ]

    MyApp.Repo.await(futures)
  end, prefix: tenant)

  [alice, bob] = result
  ```

  Conceptually, only 1 network round-trip is required, so this will be faster than
  waiting on each in sequence.

  ### Pipelining on range queries (interleaving waits)

  The functions `Repo.async_all/1` and `Repo.async_all/2` work a little bit different on the await. When the DB must
  return a list of results, if that list ends up being larger than `:erlfdb`'s configured `:target_bytes`,
  then the result set is split into multiple round-trips to the database. If your application awaits multiple
  such queries, then `:erlfdb`'s "interleaving waits" feature is used.

  Consider this example, where we're retrieving all Users and all Posts in the same transaction.

  ```elixir
  MyApp.Repo.transaction(fn ->
    futures = [
      MyApp.Repo.async_all(User),
      MyApp.Repo.async_all(Post)
    ]

    MyApp.Repo.await(futures)
  end, prefix: tenant)
  ```

  Each query individually executed would be multiple round-trips to the database as pages of results are retrieved.

  ```mermaid
  sequenceDiagram
      Client->>Server: get_range(User)
      Server-->>Client: {100 users, continuation_token}
      Client->>Server: get_range(User, continuation_token)
      Server-->>Client: {10 users, done}
  ```

  With `:erlfdb`'s interleaving waits, we send as many get_range requests as possible with pipelining
  until all are exhausted.

  ```mermaid
  sequenceDiagram
      Client->>Server: get_range(User)
      Client->>Server: get_range(Post)
      Server-->>Client: {100 users, continuation_token}
      Server-->>Client: {100 posts, continuation_token}
      Client->>Server: get_range(User, continuation_token)
      Client->>Server: get_range(Post, continuation_token)
      Server-->>Client: {10 users, done}
      Server-->>Client: {100 posts, continuation_token}
      Client->>Server: get_range(Post, continuation_token)
      Server-->>Client: {2 posts, done}
  ```

  This happens automatically when you include the list of futures to your `Repo.await/1`.

  ## Standard options

    * `:cluster_file` - The path to the fdb.cluster file. The default is
      `"/etc/foundationdb/fdb.cluster"`.
    * `:migrator` - A module that implements the `EctoFoundationDB.Migrator`
      behaviour. Required when using any indexes. Defaults to `nil`. When `nil`,
      your Repo module is assumed to be the Migrator.

  ## Advanced options

    * `:storage_id` - All tenants created by this adapter are prefixed with
      this string. This allows multiple configurations to operate on the
      same FoundationDB cluster independently. Defaults to
      `"Ecto.Adapters.FoundationDB"`.
    * `:open_db` - 1-arity function accepting the Repo module. Used for opening a reference to the
       FoundationDB cluster. Defaults to `&EctoFoundationDB.Database.open/1`. When
       using `EctoFoundationDB.Sandbox`, you should set this option to `Sandbox.open_db/1`.
    * `:migration_step` - The maximum number of keys to process in a single transaction
      when running migrations. Defaults to `1000`. If you use a number that is
      too large, the FDB transactions run by the Migrator will fail.
    * `:metadata_cache` - When set to `:enabled`, the Ecto ets cache is used to store the
      available indexes per tenant. This speeds up all database operations.
      Defaults to `:enabled`.
    * `:max_single_value_size` - Number of Bytes above which an encoded struct will
      be split into multiple FDB key-value pairs upon insert and update. Defaults to `100_000`.
      Any value beyond `100_000` is unsupported by FoundationDB. You probably do not
      want to change this value. Also see `:max_value_size`.
    * `:max_value_size` - Number of Bytes above which an encoded struct will be
      rejected by EctoFDB upon insert and update. Defaults to `:infinity`, which
      means that a single encoded struct of any size can be written to any number of
      keys. Please remember that FDB's transaction size limit of 10MB still applies.
      The maximum number of key-value pairs for a given encoded struct, not counting
      indexes, is computed by `1 + ceil(:max_value_size / :max_single_value_size)`.

  ## Optimizing for throughput and latency

  Pipelining operations inside your transactions can offer significant speed-ups, and you should
  also consider parallelizing multiple transactions. Both techniques are useful in different
  circumstances.

  - Pipelining: The transaction can contain arbitrary logic and remain ACID compliant. If you make
    smart use of pipelining, the latency of execution of that logic is kept to a minimum.
  - Parallel transactions: Each transaction is internally ACID compliant, but any 2 given
    transactions may conflict if they're accessing the same keys. When transactions do not conflict,
    throughput goes up.

  If you're attempting to get the maximum throughput on data loading, you'll probably want to
  make use of both!

  Read more at [FDB Developer Guide | Loading Data](https://apple.github.io/foundationdb/developer-guide.html#loading-data).

  ## Limitations and caveats

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
  * Deleting a field [GitHub #32](https://github.com/foundationdb-beam/ecto_foundationdb/issues/32)
  * Dropping an index
  * Moving down in migration versions (rollback)

  Finally, do not use the  Ecto Mix tasks:

  ```elixir
  # These commands are not supported. Do not use them with :ecto_foundationdb!
  #    mix ecto.create
  #    mix ecto.drop
  #    mix ecto.migrate
  #    mix ecto.gen.migration
  #    mix ecto.rollback
  #    mix ecto.migrations
  ```

  ### Key and value size

  [FoundationDB has limits on key and value size](https://apple.github.io/foundationdb/known-limitations.html#large-keys-and-values)
  Please be aware of these limitations as you develop. EctoFDB does support splitting an individual struct
  across multiple keys in the database. This is automatic and doesn't require any additional input from the developer. The
  [options](#module-advanced-options) `:max_single_value_size` and `:max_value_size` allow you to tweak this feature.


  ### Ecto queries

  You'll find that most queries outside of those detailed in the documentation fail with
  an `EctoFoundationDB.Exception.Unsupported` exception. The FoundationDB Layer concept
  precludes complex queries from being executed within the database. Therefore, it only makes sense
  to implement a limited set of query types -- specifically those that do have optimized database
  query semantics. All other filtering, aggregation, and grouping must be done by your Elixir code.

  In other words, you'll be writing less SQL, and more Elixir.

  Refer to the integration tests for a collection of queries that is known to work.

  ### Other Ecto features

  As you test the boundaries of EctoFoundationDB, you'll find that many other Ecto features just plain
  don't work with this adapter. Ecto has a lot of features, and this adapter is in active development.
  Please let us know what you find!
  """

  @behaviour Ecto.Adapter
  @behaviour Ecto.Adapter.Schema
  @behaviour Ecto.Adapter.Queryable
  @behaviour Ecto.Adapter.Transaction

  alias EctoFoundationDB.Database
  alias EctoFoundationDB.Future
  alias EctoFoundationDB.Layer.Tx
  alias EctoFoundationDB.Options
  alias EctoFoundationDB.Tenant

  alias Ecto.Adapters.FoundationDB.EctoAdapter
  alias Ecto.Adapters.FoundationDB.EctoAdapterAssigns
  alias Ecto.Adapters.FoundationDB.EctoAdapterAsync
  alias Ecto.Adapters.FoundationDB.EctoAdapterQueryable
  alias Ecto.Adapters.FoundationDB.EctoAdapterSchema
  alias Ecto.Adapters.FoundationDB.EctoAdapterTransaction

  @spec db(Ecto.Repo.t()) :: Database.t()
  def db(repo) when is_atom(repo) do
    case :persistent_term.get({__MODULE__, repo, :database}, nil) do
      nil ->
        options = repo.config()
        fun = Options.get(options, :open_db)
        db = fun.(repo)
        :persistent_term.put({__MODULE__, repo, :database}, {db, options})
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
  @spec transactional(Tenant.t() | nil, function()) :: any()
  def transactional(tenant, fun), do: Tx.transactional_external(tenant, fun)

  @impl Ecto.Adapter
  defmacro __before_compile__(_env) do
    quote do
      def async_all_range(queryable, id_s, id_e, opts \\ []) do
        async_query(fn ->
          repo = get_dynamic_repo()

          EctoAdapterQueryable.execute_all_range(
            __MODULE__,
            repo,
            queryable,
            id_s,
            id_e,
            Ecto.Repo.Supervisor.tuplet(
              repo,
              Keyword.merge(
                __MODULE__.default_options(:all),
                opts
              )
            )
          )
        end)
      end

      def all_range(queryable, id_s, id_e, opts \\ []) do
        future = async_all_range(queryable, id_s, id_e, opts)
        await(future)
      end

      def async_get(queryable, id, opts \\ []),
        do: async_query(fn -> get(queryable, id, opts ++ [returning: {:future, :one}]) end)

      def async_get!(queryable, id, opts \\ []),
        do: async_query(fn -> get!(queryable, id, opts ++ [returning: {:future, :one}]) end)

      def async_get_by(queryable, clauses, opts \\ []),
        do:
          async_query(fn -> get_by(queryable, clauses, opts ++ [returning: {:future, :one}]) end)

      def async_get_by!(queryable, clauses, opts \\ []),
        do:
          async_query(fn -> get_by!(queryable, clauses, opts ++ [returning: {:future, :one}]) end)

      def async_one(queryable, opts \\ []),
        do: async_query(fn -> one(queryable, opts ++ [returning: {:future, :one}]) end)

      def async_one!(queryable, opts \\ []),
        do: async_query(fn -> one!(queryable, opts ++ [returning: {:future, :one}]) end)

      def async_all(queryable, opts \\ []),
        do: async_query(fn -> all(queryable, opts ++ [returning: {:future, :all}]) end)

      defp async_query(fun) do
        repo = get_dynamic_repo()
        EctoAdapterAsync.async_query(__MODULE__, repo, fun)
      end

      def await(futures) when is_list(futures) do
        futures
        |> Future.await_stream()
        |> Stream.map(&Future.result/1)
        |> Enum.to_list()
      end

      def await(future) do
        [res] = await([future])
        res
      end

      def watch(struct, options \\ []) do
        repo = get_dynamic_repo()

        EctoAdapterSchema.watch(
          __MODULE__,
          repo,
          struct,
          Ecto.Repo.Supervisor.tuplet(
            repo,
            Keyword.merge(__MODULE__.default_options(:watch), options)
          )
        )
      end

      def assign_ready(futures, ready_refs, options \\ []) do
        repo = get_dynamic_repo()

        EctoAdapterAssigns.assign_ready(
          __MODULE__,
          repo,
          futures,
          ready_refs,
          options
        )
      end

      def async_assign_ready(futures, ready_ref, options \\ []) do
        repo = get_dynamic_repo()

        EctoAdapterAssigns.async_assign_ready(
          __MODULE__,
          repo,
          futures,
          ready_ref,
          options
        )
      end
    end
  end

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
