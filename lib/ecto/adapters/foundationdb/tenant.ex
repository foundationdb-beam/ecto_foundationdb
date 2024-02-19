defmodule Ecto.Adapters.FoundationDB.Tenant do
  @moduledoc """
  This module allows the application to manage the creation and deletion of
  tenants within the FoundationDB database. All transactions require a tenant,
  so any application that uses the Ecto FoundationDB Adapter must use this module.
  """
  alias Ecto.Adapters.FoundationDB, as: FDB
  alias Ecto.Adapters.FoundationDB.EctoAdapterStorage

  def open!(repo, id), do: open!(FDB.db(repo), id, repo.config())

  def exists?(repo, id), do: exists?(FDB.db(repo), id, repo.config())
  def open(repo, id), do: open(FDB.db(repo), id, repo.config())
  def list(repo), do: list(FDB.db(repo), repo.config())
  def create(repo, id), do: create(FDB.db(repo), id, repo.config())
  def clear(repo, id), do: clear(FDB.db(repo), id, repo.config())
  def delete(repo, id), do: delete(FDB.db(repo), id, repo.config())

  def open!(db, id, options) do
    :ok = ensure_created(db, id, options)
    open(db, id, options)
  end

  def open_empty!(db, id, options) do
    :ok = ensure_created(db, id, options)
    :ok = empty(db, id, options)
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

  def list(db, options) do
    for {_k, json} <- EctoAdapterStorage.list_tenants(db, options) do
      %{"name" => %{"printable" => name}} = Jason.decode!(json)
      EctoAdapterStorage.tenant_name_to_id!(name, options)
    end
  end

  def create(db, id, options), do: EctoAdapterStorage.create_tenant(db, id, options)
  def clear(db, id, options), do: EctoAdapterStorage.clear_tenant(db, id, options)
  def empty(db, id, options), do: EctoAdapterStorage.empty_tenant(db, id, options)
  def delete(db, id, options), do: EctoAdapterStorage.delete_tenant(db, id, options)
end
