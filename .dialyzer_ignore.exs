[
  # Hint: mix dialyzer --format ignore_file_strict

  # We don't implement all of the Migration behaviour's possible DDLs, so ignore these errors
  {"lib/ecto/adapters/foundationdb.ex", "Type mismatch with behaviour callback to execute_ddl/3."},
  {"lib/ecto/adapters/foundationdb/ecto_adapter_migration.ex", "Type mismatch with behaviour callback to execute_ddl/3."}
]
