defmodule EctoFoundationDB.Options do
  @moduledoc """
  Options for `Ecto.Adapters.FoundationDB`.
  """

  @type option() ::
          {:open_db, function()}
          | {:storage_id, String.t()}
          | {:open_tenant_callback, function()}
          | {:migrator, module()}
          | {:cluster_file, :erlfdb.cluster_filename()}
          | {:migration_step, integer()}

  @type t() :: [option()]

  alias EctoFoundationDB.Exception.Unsupported

  @spec get(t(), atom()) :: any()
  def get(options, :open_db) do
    Keyword.get(options, :open_db, fn -> :erlfdb.open(get(options, :cluster_file)) end)
  end

  def get(options, :storage_id),
    do: Keyword.get(options, :storage_id, "Ecto.Adapters.FoundationDB")

  def get(options, :cluster_file), do: Keyword.get(options, :cluster_file, "")

  def get(options, :migrator),
    do: Keyword.get(options, :migrator, nil)

  def get(options, :migration_step),
    do: Keyword.get(options, :migration_step, 1000)

  def get(options, :idx_cache),
    do: Keyword.get(options, :idx_cache, :enabled)

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
