defmodule Ecto.Adapters.FoundationDB.EctoAdapterMigration do
  @moduledoc """
  Ecto.Adapter.Migration lives in ecto_sql, so it has SQL behavior baked in.
  We'll do our best to translate into FoundationDB operations so that migrations
  are minimally usable. The experience might feel a little strange though.
  """
  @behaviour Ecto.Adapter.Migration

  @impl true
  def supports_ddl_transaction?() do
    # TODO: maybe support this?
    false
  end

  @impl true
  def execute_ddl(adapter_meta, command, options) do
    # %{pid: #PID<0.304.0>, opts: [repo: Ecto.Integration.TestRepo, telemetry_prefix: [:ecto, :integration, :test_repo], otp_app: :ecto_foundationdb, timeout: 15000, pool_size: 10, key_delimiter: "/"], cache: #Reference<0.189210129.853671937.148759>, stacktrace: nil, repo: Ecto.Integration.TestRepo, telemetry: {Ecto.Integration.TestRepo, :debug, [:ecto, :integration, :test_repo, :query]}, adapter: Ecto.Adapters.FoundationDB}
    # {:create_if_not_exists, %Ecto.Migration.Table{name: :schema_migrations, prefix: nil, comment: nil, primary_key: true, engine: nil, options: nil}, [{:add, :version, :bigint, [primary_key: true]}, {:add, :inserted_at, :naive_datetime, []}]}
    # [timeout: :infinity, log: false, schema_migration: true, telemetry_options: [schema_migration: true]]

    raise """
    execute_ddl

    #{inspect(adapter_meta)}
    #{inspect(command)}
    #{inspect(options)}
    """
  end

  @impl true
  def lock_for_migrations(adapter_meta, options, fun) do
    raise """
    lock_for_migration

    #{inspect(adapter_meta)}
    #{inspect(options)}
    #{inspect(fun)}
    """
  end
end
