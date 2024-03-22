defmodule Ecto.Adapters.FoundationDB.EctoAdapterMigration do
  @moduledoc """
  Ecto.Adapter.Migration lives in ecto_sql, so it has SQL behavior baked in.
  We'll do our best to translate into FoundationDB operations so that migrations
  are minimally usable. The experience might feel a little strange though.
  """
  @behaviour Ecto.Adapters.FoundationDB.Migration

  alias Ecto.Adapters.FoundationDB.Exception.Unsupported
  alias Ecto.Adapters.FoundationDB.Layer.IndexInventory
  alias Ecto.Adapters.FoundationDB.Migration.Index

  @impl true
  def supports_ddl_transaction?() do
    false
  end

  @impl true

  def execute_ddl(
        adapter_meta = %{opts: _adapter_opts},
        {:create,
         %Index{
           prefix: tenant,
           table: source,
           name: index_name,
           columns: index_fields,
           options: options
         }},
        _options
      ) do
    :ok =
      IndexInventory.create_index(
        tenant,
        adapter_meta,
        source,
        index_name,
        index_fields,
        options
      )

    {:ok, []}
  end

  def execute_ddl(
        _adapter_meta,
        unsupported,
        _options
      ) do
    raise Unsupported, "Migration DDL not supported #{inspect(unsupported)}"
  end
end
