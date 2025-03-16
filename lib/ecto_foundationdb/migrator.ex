defmodule EctoFoundationDB.Migrator do
  @moduledoc """
  Implement this behaviour to define migrations for `Ecto.Adapters.FoundationDB`
  """

  require Logger

  @callback migrations() :: [{non_neg_integer(), module()}]

  alias Ecto.Adapters.FoundationDB
  alias EctoFoundationDB.MigrationsPJ
  alias EctoFoundationDB.Options
  alias EctoFoundationDB.Tenant

  @doc false
  defmacro __using__(_) do
    quote location: :keep do
      import EctoFoundationDB.Migrator
      @before_compile EctoFoundationDB.Migrator
      @behaviour EctoFoundationDB.Migrator
    end
  end

  @doc false
  defmacro __before_compile__(_env) do
    quote do
      def __migrator__ do
        []
      end
    end
  end

  @spec up(Ecto.Repo.t(), Tenant.t() | Tenant.id(), Options.t()) :: :ok
  def up(repo, tenant_id, options) when is_binary(tenant_id) do
    db = FoundationDB.db(repo)
    tenant = Tenant.Backend.db_open(db, tenant_id, options)
    up(repo, tenant, options)
  end

  def up(repo, tenant, options) do
    migrator = Options.get(options, :migrator)
    migrator = if is_nil(migrator), do: repo, else: migrator
    {:module, _} = Code.ensure_loaded(migrator)
    migrations? = Kernel.function_exported?(migrator, :migrations, 0)

    if migrations? do
      limit = Options.get(options, :migration_step)
      MigrationsPJ.transactional(repo, tenant, migrator, limit, options)
    else
      :ok
    end

    :ok
  end
end
