defmodule EctoFoundationDB.Tenant.DirectoryTenant do
  @moduledoc """
  The default backend for EctoFDB multitenancy.

  It uses the Directory Layer defined by `:erlfdb_directory` to
  partition tenant keyspaces from each other. There is no guarantee
  provided by FDB at the transaction-level, so care must be taken to
  always pack keys appropriately. Standard use of Ecto.Repo functions
  is guaranteed to respect tenant boundaries.
  """
  defstruct [:ref, :node, :prefix]

  alias EctoFoundationDB.Options
  alias EctoFoundationDB.Tenant.Backend

  @behaviour Backend

  @type t() :: %__MODULE__{}

  @impl true
  def txobj(db, _opened, _meta), do: db
  @impl true
  def ref(_opened, %__MODULE__{ref: ref}), do: ref
  @impl true
  def make_meta(node), do: %__MODULE__{ref: make_ref(), node: node, prefix: prefix(node)}
  @impl true
  def get_name(id, _options), do: id
  @impl true
  def list(db, options), do: :erlfdb_directory.list(db, tenant_node(db, options))

  @impl true
  def create(db, "", options) do
    tenant_node(db, options)
    :ok
  end

  def create(db, tenant_name, options) do
    :erlfdb_directory.create(db, tenant_node(db, options), tenant_name)
    :ok
  rescue
    e in ErlangError ->
      case e do
        %ErlangError{
          original: {:erlfdb_directory, {:create_error, :path_exists, _path}}
        } ->
          {:error, :tenant_already_exists}
      end
  end

  @impl true
  def delete(db, "", options) do
    tenant_dir_name = "#{Options.get(options, :storage_id)}"
    :persistent_term.erase({__MODULE__, tenant_dir_name})

    try do
      :erlfdb_directory.remove(db, root_node(), tenant_dir_name)
    rescue
      e in ErlangError ->
        case e do
          %ErlangError{
            original: {:erlfdb_directory, {:delete_error, :path_missing, _path}}
          } ->
            :ok
        end
    end
  end

  def delete(db, tenant_name, options) do
    :erlfdb_directory.remove(db, tenant_node(db, options), tenant_name)
  rescue
    e in ErlangError ->
      case e do
        %ErlangError{
          original: {:erlfdb_directory, {:delete_error, :path_missing, _path}}
        } ->
          :ok
      end
  end

  @impl true
  def get(db, "", options) do
    tenant_dir_name = "#{Options.get(options, :storage_id)}"

    if :erlfdb_directory.exists(db, root_node(), tenant_dir_name) do
      {:ok, tenant_node(db, options)}
    else
      {:error, :tenant_does_not_exist}
    end
  end

  def get(db, tenant_name, options) do
    if :erlfdb_directory.exists(db, tenant_node(db, options), tenant_name) do
      {:ok, open(db, tenant_name, options)}
    else
      {:error, :tenant_does_not_exist}
    end
  end

  @impl true
  def open(db, "", options), do: tenant_node(db, options)

  def open(db, tenant_name, options),
    do: :erlfdb_directory.open(db, tenant_node(db, options), tenant_name)

  @impl true
  def all_data_ranges(meta) do
    prefix = prefix(meta.node)

    [
      # subspace keys (how this adapter encourages the user to write custom data)
      {prefix, :erlfdb_key.strinc(prefix)},

      # tupled keys (how this adapter writes Ecto data)
      :erlfdb_tuple.range({prefix})
    ]
  end

  @impl true
  def get_id({{:utf8, id}, _node}, _options), do: id

  @impl true
  def extend_tuple(x, meta), do: add_tuple_head(x, meta.prefix)

  @impl true
  def extract_tuple(tuple, _meta), do: delete_tuple_head(tuple)

  defp add_tuple_head(tuple, head) when is_tuple(tuple) do
    :erlang.insert_element(1, tuple, head)
  end

  defp add_tuple_head(list, head) when is_list(list) do
    [head | list]
    |> :erlang.list_to_tuple()
  end

  defp add_tuple_head(function, head) when is_function(function) do
    function.(1)
    |> add_tuple_head(head)
  end

  defp delete_tuple_head(tuple) do
    :erlang.delete_element(1, tuple)
  end

  defp tenant_node(db, options) do
    tenant_dir_name = "#{Options.get(options, :storage_id)}"

    case :persistent_term.get({__MODULE__, tenant_dir_name}, nil) do
      nil ->
        tenant_dir = :erlfdb_directory.create_or_open(db, root_node(), tenant_dir_name)
        :persistent_term.put({__MODULE__, tenant_dir_name}, tenant_dir)
        tenant_dir

      tenant_dir ->
        tenant_dir
    end
  end

  defp root_node() do
    case :persistent_term.get({__MODULE__, :root}, nil) do
      nil ->
        root = :erlfdb_directory.root(node_prefix: <<0xFE>>, content_prefix: <<>>)
        :persistent_term.put({__MODULE__, :root}, root)
        root

      root ->
        root
    end
  end

  defp prefix(node) do
    subspace = :erlfdb_directory.get_subspace(node)
    :erlfdb_subspace.key(subspace)
  end
end
