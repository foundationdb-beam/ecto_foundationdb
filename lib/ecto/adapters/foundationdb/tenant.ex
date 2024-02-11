defmodule Ecto.Adapters.FoundationDB.Tenant do
  alias Ecto.Adapters.FoundationDB.EctoAdapterStorage

  def open!(db, id, options) do
    :ok = ensure_created(db, id, options)
    open(db, id, options)
  end

  def clear_open!(db, id, options) do
    :ok = ensure_created(db, id, options)
    :ok = clear(db, id, options)
    open(db, id, options)
  end

  def clear_delete!(db, id, options) do
    if exists?(db, id, options) do
      :ok = clear(db, id, options)
      :ok = delete(db, id, options)
    end
    :ok
  end

  def ensure_created(db, id, options) do
    case exists?(db, id, options) do
      true -> :ok
      false -> create(db, id, options)
    end
  end

  def exists?(db, id, options), do: EctoAdapterStorage.tenant_exists?(db, id, options)
  def open(db, id, options), do: EctoAdapterStorage.open_tenant(db, id, options)
  def list(db, options), do: EctoAdapterStorage.list_tenants(db, options)
  def create(db, id, options), do: EctoAdapterStorage.create_tenant(db, id, options)
  def clear(db, id, options), do: EctoAdapterStorage.clear_tenant(db, id, options)
  def delete(db, id, options), do: EctoAdapterStorage.delete_tenant(db, id, options)
end
