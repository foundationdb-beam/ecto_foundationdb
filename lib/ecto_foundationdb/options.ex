defmodule EctoFoundationDB.Options do
  @moduledoc "See `Ecto.Adapters.FoundationDB`"

  @type option() ::
          {:open_db, function()}
          | {:storage_id, String.t()}
          | {:storage_delimiter, String.t()}
          | {:open_tenant_callback, function()}
          | {:migrator, module()}
          | {:cluster_file, :erlfdb.cluster_filename()}
          | {:migration_step, integer()}
          | {:max_single_value_size, integer()}
          | {:max_value_size, integer()}
          | {:log, boolean()}

  @fdb_max_single_value_size_bytes 100_000

  # :migration_step must be large enough to fit the largest multikey value. Otherwise, the ProgressiveJob
  # cannot make progress. 1000 is safe with other defaults.
  @migration_step 1000

  @type t() :: [option()]

  alias EctoFoundationDB.Exception.Unsupported

  @spec get(t(), atom()) :: any()
  def get(options, :open_db) do
    Keyword.get(options, :open_db, &EctoFoundationDB.Database.open/1)
  end

  def get(options, :storage_id),
    do: Keyword.get(options, :storage_id, "Ecto.Adapters.FoundationDB")

  def get(options, :storage_delimiter),
    do: Keyword.get(options, :storage_delimiter, "/")

  def get(options, :tenant_backend),
    do: Keyword.get(options, :tenant_backend, EctoFoundationDB.Tenant.DirectoryTenant)

  def get(options, :cluster_file), do: Keyword.get(options, :cluster_file, "")

  def get(options, :migrator),
    do: Keyword.get(options, :migrator, nil)

  def get(options, :migration_step),
    do: Keyword.get(options, :migration_step, @migration_step)

  def get(options, :metadata_cache),
    do: Keyword.get(options, :metadata_cache, :enabled)

  def get(options, :max_single_value_size),
    do: Keyword.get(options, :max_single_value_size, @fdb_max_single_value_size_bytes)

  def get(options, :max_value_size),
    do: Keyword.get(options, :max_value_size, :infinity)

  def get(options, :log),
    do: Keyword.get(options, :log, true)

  def get(options, key),
    do:
      get_or_raise(
        options,
        key,
        "FoundationDB Adapter does not specify a default for option #{inspect(key)}"
      )

  defp get_or_raise(options, key, message) do
    case options[key] do
      nil ->
        raise Unsupported, message

      val ->
        val
    end
  end
end
