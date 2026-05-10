defmodule EctoFoundationDB.TenantCache do
  @moduledoc false
  use GenServer

  alias EctoFoundationDB.Options

  defstruct [:table]

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, [])
  end

  def start_link(args, opts) do
    GenServer.start_link(__MODULE__, args, opts)
  end

  def init([config]) do
    storage_id = Options.get(config, :storage_id)
    name = cache_name(storage_id)

    try do
      table = :erlfdb_directory_cache.new(name)
      # timer:apply_interval(Ttl, erlfdb_directory_cache, purge, [Table, Ttl])
      {:ok, %__MODULE__{table: table}}
    catch
      :error, :badarg ->
        case :ets.whereis(name) do
          :undefined ->
            exit(:badarg)

          _tid ->
            # :storage_id can span across Repos, although it's not recommended. So,
            # another repo has already started the TenantCache and this one should
            # exit silently.
            :ignore
        end
    end
  end

  def cache_name(storage_id) do
    String.to_atom("erlfdb_directory_cache:#{storage_id}")
  end
end
