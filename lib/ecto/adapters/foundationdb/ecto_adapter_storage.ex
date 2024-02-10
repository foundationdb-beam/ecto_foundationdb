defmodule Ecto.Adapters.FoundationDB.EctoAdapterStorage do
  @behaviour Ecto.Adapter.Storage

  alias Ecto.Adapters.FoundationDB.Exception.Unsupported

  @impl true
  def storage_up(options) do
    db = open_db(options)
    tenant_name = get_tenant_name(options)
    case open_storage_tenant(db, tenant_name, options) do
      {:error, :storage_tenant_does_not_exist} ->
        :ok = :erlfdb_tenant_management.create_tenant(db, tenant_name)
        :ok
      _storage_tenant ->
        {:error, :already_up}
    end
  end

  @impl true
  def storage_down(options) do
    db = open_db(options)
    tenant_name = get_tenant_name(options)
    case open_storage_tenant(db, tenant_name, options) do
      {:error, :stoage_tenant_does_not_exist} ->
        {:error, :already_down}
      _storage_tenant ->
        :ok = :erlfdb_tenant_management.delete_tenant(db, tenant_name)
        :ok
    end
  end

  @impl true
  def storage_status(options) do
    case open_storage_tenant(options) do
      {:error, :storage_tenant_does_not_exist} ->
        :down
      _storage_tenant ->
        :up
    end
  end

  defp open_db(options) do
    case options[:open_db] do
      nil ->
        raise Unsupported, """
        FoundationDB Adapter does not implement a default value for :open_db
        """
      fun ->
        fun.()
    end
  end

  defp get_tenant_name(options) do
    case options[:storage_tenant] do
      nil ->
        raise Unsupported, """
        FoundationDB Adapter does not implement a default value for :storage_tenant
        """
      storage_tenant ->
        "#{storage_tenant}"
    end
  end

  defp open_storage_tenant(options) do
    options
    |> open_db()
    |> open_storage_tenant()
  end

  defp open_storage_tenant(db, tenant_name, _options) do
      case :erlfdb_tenant_management.get_tenant(db, tenant_name) do
        :not_found ->
          {:error, :storage_tenant_does_not_exist}
        _ ->
          :erlfdb.open_tenant(db, tenant_name)
      end
  end
end
