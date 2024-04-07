defmodule Ecto.Adapters.FoundationDB.EctoAdapterStorage do
  @moduledoc false
  @behaviour Ecto.Adapter.Storage

  alias EctoFoundationDB.Layer.Pack
  alias EctoFoundationDB.Options

  @all_data_start_key ""
  @all_data_end_key <<0xFF>>

  def list_tenants(dbtx, options) do
    start_key = get_tenant_name("", options)
    end_key = :erlfdb_key.strinc(start_key)
    :erlfdb_tenant_management.list_tenants(dbtx, start_key, end_key, [])
  end

  def tenant_exists?(dbtx, tenant_id, options) do
    case get_tenant(dbtx, tenant_id, options) do
      {:ok, _} -> true
      {:error, :tenant_does_not_exist} -> false
    end
  end

  def tenant_name_to_id!(tenant_name, options) do
    prefix = get_tenant_name("", options)
    len = String.length(prefix)
    ^prefix = String.slice(tenant_name, 0, len)
    String.slice(tenant_name, len, String.length(tenant_name) - len)
  end

  def create_tenant(dbtx, tenant_id, options) do
    tenant_name = get_tenant_name(tenant_id, options)

    try do
      :erlfdb_tenant_management.create_tenant(dbtx, tenant_name)
    rescue
      e in ErlangError ->
        case e do
          %ErlangError{original: {:erlfdb_error, 2132}} ->
            {:error, :tenant_already_exists}
        end
    end
  end

  def open_tenant(db, tenant_id, options) do
    tenant_name = get_tenant_name(tenant_id, options)
    open_named_tenant(db, tenant_name)
  end

  def delete_tenant(dbtx, tenant_id, options) do
    tenant_name = get_tenant_name(tenant_id, options)

    try do
      :erlfdb_tenant_management.delete_tenant(dbtx, tenant_name)
    rescue
      e in ErlangError ->
        case e do
          %ErlangError{original: {:erlfdb_error, 2133}} ->
            {:error, :tenant_nonempty}
        end
    end
  end

  def empty_tenant(dbtx, tenant_id, options) do
    tenant = open_tenant(dbtx, tenant_id, options)

    :erlfdb.transactional(tenant, fn tx ->
      {start_key, end_key} = Pack.adapter_repo_range()
      :erlfdb.clear_range(tx, start_key, end_key)
    end)

    :ok
  end

  def clear_tenant(dbtx, tenant_id, options) do
    tenant = open_tenant(dbtx, tenant_id, options)

    :erlfdb.transactional(tenant, fn tx ->
      :erlfdb.clear_range(tx, @all_data_start_key, @all_data_end_key)
    end)

    :ok
  end

  @impl true
  def storage_up(options) do
    db = open_db(options)
    tenant_name = get_storage_tenant_name(options)

    case get_named_tenant(db, tenant_name) do
      {:error, :tenant_does_not_exist} ->
        :ok = :erlfdb_tenant_management.create_tenant(db, tenant_name)
        :ok

      {:ok, _} ->
        {:error, :already_up}
    end
  end

  @impl true
  def storage_down(options) do
    db = open_db(options)
    tenant_name = get_storage_tenant_name(options)

    case get_named_tenant(db, tenant_name) do
      {:error, :tenant_does_not_exist} ->
        {:error, :already_down}

      {:ok, _} ->
        tenant = open_named_tenant(db, tenant_name)

        :erlfdb.transactional(tenant, fn tx ->
          :erlfdb.clear_range(tx, @all_data_start_key, @all_data_end_key)
        end)

        :ok = :erlfdb_tenant_management.delete_tenant(db, tenant_name)
        :ok
    end
  end

  @impl true
  def storage_status(options) do
    db = open_db(options)
    storage_tenant_name = get_storage_tenant_name(options)

    case get_named_tenant(db, storage_tenant_name) do
      {:ok, _} ->
        :up

      _ ->
        :down
    end
  end

  def open_db(options) do
    fun = Options.get(options, :open_db)
    fun.()
  end

  def open_storage_tenant(db, options) do
    tenant_name = get_storage_tenant_name(options)
    open_named_tenant(db, tenant_name)
  end

  defp get_storage_tenant_name(options) do
    storage_id = Options.get(options, :storage_id)
    "#{storage_id}"
  end

  defp get_tenant_name(tenant_id, options) do
    storage_id = Options.get(options, :storage_id)

    "#{storage_id}/" <> tenant_id
  end

  defp get_tenant(dbtx, tenant_id, options) do
    tenant_name = get_tenant_name(tenant_id, options)
    get_named_tenant(dbtx, tenant_name)
  end

  defp get_named_tenant(dbtx, tenant_name) do
    case :erlfdb_tenant_management.get_tenant(dbtx, tenant_name) do
      :not_found ->
        {:error, :tenant_does_not_exist}

      tenant ->
        {:ok, tenant}
    end
  end

  defp open_named_tenant(db, tenant_name) do
    :erlfdb.open_tenant(db, tenant_name)
  end
end
