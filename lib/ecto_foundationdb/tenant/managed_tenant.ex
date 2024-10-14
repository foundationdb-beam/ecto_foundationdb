defmodule EctoFoundationDB.Tenant.ManagedTenant do
  @moduledoc """
  An experimental backend for EctoFDB multitenancy. It uses FDB's Tenants
  to guarantee that a given transaction cannot access keys from another tenant.

  To use ManagedTenant, your database must be configured with

  ```shell
  fdbcli --exec 'configure tenant_mode=required_experimental'
  # or
  fdbcli --exec 'configure tenant_mode=optional_experimental'
  ```

  FDB's tenants are still an experimental feature, so EctoFDB's ManagedTenant must
  also be considered experimental. For now, we recommend using `EctoFoundationDB.Tenant.DirectoryTenant`,
  which is the default.
  """
  defstruct []

  alias EctoFoundationDB.Options
  alias EctoFoundationDB.Tenant.Backend

  @behaviour Backend

  @type t() :: %__MODULE__{}

  @impl true
  def txobj(_db, tenant_ref, _meta) do
    tenant_ref
  end

  @impl true
  def ref({:erlfdb_tenant, tenant_ref}, _meta), do: tenant_ref

  @impl true
  def make_meta(_tenant_ref) do
    %__MODULE__{}
  end

  @impl true
  def get_name(id, options) do
    storage_id = Options.get(options, :storage_id)
    storage_delimiter = Options.get(options, :storage_delimiter)

    "#{storage_id}#{storage_delimiter}#{id}"
  end

  @impl true
  def list(db, options) do
    start_name = get_name("", options)
    end_name = :erlfdb_key.strinc(start_name)
    :erlfdb_tenant_management.list_tenants(db, start_name, end_name, options)
  end

  @impl true
  def create(db, tenant_name, _options) do
    :erlfdb_tenant_management.create_tenant(db, tenant_name)
  rescue
    e in ErlangError ->
      case e do
        %ErlangError{original: {:erlfdb_error, 2132}} ->
          {:error, :tenant_already_exists}
      end
  end

  @impl true
  def delete(db, tenant_name, _options) do
    :erlfdb_tenant_management.delete_tenant(db, tenant_name)
  rescue
    e in ErlangError ->
      case e do
        %ErlangError{
          original: {:erlfdb_directory, {:remove_error, :path_missing, [utf8: ^tenant_name]}}
        } ->
          {:error, :tenant_nonempty}
      end
  end

  @impl true
  def get(db, tenant_name, _options) do
    case :erlfdb_tenant_management.get_tenant(db, tenant_name) do
      :not_found ->
        {:error, :tenant_does_not_exist}

      tenant ->
        {:ok, tenant}
    end
  end

  @impl true
  def open(db, tenant_name, _options) do
    :erlfdb.open_tenant(db, tenant_name)
  end

  @impl true
  def all_data_ranges(_meta) do
    [{"", <<0xFF>>}]
  end

  @impl true
  def get_id({_key, json}, options) do
    %{"name" => %{"printable" => name}} = Jason.decode!(json)
    tenant_name_to_id!(name, options)
  end

  @impl true
  def extend_tuple(tuple, _meta) when is_tuple(tuple), do: tuple

  def extend_tuple(function, meta) when is_function(function),
    do: function.(0) |> extend_tuple(meta)

  def extend_tuple(list, _meta) when is_list(list), do: :erlang.list_to_tuple(list)

  @impl true
  def extract_tuple(tuple, _meta), do: tuple

  defp tenant_name_to_id!(tenant_name, options) do
    prefix = get_name("", options)
    len = String.length(prefix)
    ^prefix = String.slice(tenant_name, 0, len)
    String.slice(tenant_name, len, String.length(tenant_name) - len)
  end
end
