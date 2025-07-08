# Operator's Manual

In order to maintain transactional guarantees as your application changes and
grows, there is a burden on the database operator for some data manipulations.
EctoFoundationDB provides building blocks that
can be executed at strategic times to maintain data integrity.

There is a need for a human operator to execute these commands for one
important reason: EctoFDB does not know details about the topology of your
distributed application, or the migration versions that are active on clients
at a given time.

In writing this guide, we've taken heavy inspiration from
[GitHub | Safe Ecto Migrations](https://github.com/fly-apps/safe-ecto-migrations)
It's written assuming a SQL database like Postgres, but many of the concepts
are applicable to EctoFoundationDB, and it's worth a read.

> #### Safe Index Migrations {: .note}
> Adding and dropping indexes is done completely on-line without risk of data consistency problems.
> The remainder of this document discusses other data operations. Please
> refer to the documentation on
> [Migrations](Ecto.Adapters.FoundationDB.html#module-migrations)
> for more info on index migrations.

## Safe Data Migrations

- [Adding a field](#adding-a-field)
- [Removing a field](#removing-a-field)
- [Renaming a field](#renaming-a-field)

### Adding a field

Adding a field is trivial. Simply modify your schema to include the new field.
There is no `ALTER TABLE` DDL or equivalent. Any existing objects will
automatically interpret values for the new field as `nil`. If you require
some other value, use the `Repo.update!/2` function like normal.

```diff
# App deploy, in the Ecto schema

defmodule MyApp.Post do
  schema "posts" do
+   field :a_new_field, :string
  end
end
```

---

### Removing a field

If all you care about is not having the field show up in your structs, you may
choose to simply remove it from your schema. The underlying data will remain
intact, however, until each item is updated at some point in the future. If you
need to ensure that the data is truly removed, continue reading.

Safety can be assured if the application code is first updated to remove references to the field so it's no longer loaded or queried. Then, the field can safely be removed from the database.

1. Deploy code change to remove references to the field.
2. Execute `EctoFoundationDB.CLI.delete_field!/3` to remove the data.

> #### Warning: data rewrite {: .warning}
> Removing a field requires writing all keys that have that field in the
> value.

Application deployment:

```diff
# App deploy, in the Ecto schema

defmodule MyApp.Post do
  schema "posts" do
-   field :no_longer_needed, :string
  end
end
```

Once your application is fully deployed to all nodes, execute the following
command:

```elixir
iex> EctoFoundationDB.CLI.delete_field!(MyApp.Repo, MyApp.Post, :no_longer_needed)
```

---

### Renaming a field

Take a phased approach:

1. Create a new field with any relevant indexes
2. In application code, write to both fields
3. Backfill data from old field to new field (see below)
4. In application code, move reads from old field to the new field
5. In application code, remove old field from Ecto schemas, and drop any old index(es)
6. Delete the old field (see below)

> #### Warning: data rewrite {: .warning}
> Renaming a field requires writing all related keys twice.

**Backfill data from old field to new field**

```elixir
iex> EctoFoundationDB.CLI.copy_field!(MyApp.Repo, MyApp.Post, :old_field, :new_field)
```

**Delete the old field**

```elixir
iex> EctoFoundationDB.CLI.delete_field!(MyApp.Repo, MyApp.Post, :old_field)
```

> #### Sample Available {: .note}
> Please refer to [GitHub | cli_test.exs](https://github.com/foundationdb-beam/ecto_foundationdb/blob/main/test/ecto/integration/cli_test.exs)
> to review the test that executes this operation.
