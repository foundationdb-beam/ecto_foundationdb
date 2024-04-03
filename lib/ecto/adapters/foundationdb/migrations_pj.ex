defmodule Ecto.Adapters.FoundationDB.MigrationsPJ do
  @moduledoc false
  alias Ecto.Adapters.FoundationDB.Schema
  alias Ecto.Adapters.FoundationDB.Layer.Pack
  alias Ecto.Adapters.FoundationDB.ProgressiveJob
  alias Ecto.Adapters.FoundationDB.Layer.Indexer
  alias Ecto.Adapters.FoundationDB.Layer.IndexInventory
  alias Ecto.Adapters.FoundationDB.Migration.Index
  alias Ecto.Adapters.FoundationDB.Migration.SchemaMigration
  @behaviour Ecto.Adapters.FoundationDB.ProgressiveJob

  defmodule State do
    defstruct [:repo, :final_version, :limit]
  end

  alias __MODULE__.State

  def claim_key(), do: Pack.namespaced_pack(SchemaMigration.source(), "claim", [])

  def transactional(repo, tenant, migrator, limit) do
    active_versions = repo.all(SchemaMigration.versions(), prefix: tenant)

    ProgressiveJob.new(tenant, __MODULE__, %{
      active_versions: active_versions,
      repo: repo,
      migrator: migrator,
      limit: limit
    })
    |> ProgressiveJob.transactional_stream()
    |> Enum.to_list()
  end

  @impl true
  def init(%{active_versions: active_versions, repo: repo, migrator: migrator, limit: limit}) do
    latest_active_version = Enum.max(active_versions, &>=/2, fn -> -1 end)

    migrations = migrator.migrations()

    new_migrations =
      migrations
      |> Enum.filter(fn
        {mig_vsn, _} when mig_vsn > latest_active_version -> true
        _ -> false
      end)
      |> Enum.sort()
      |> Enum.map(fn {vsn, mod} ->
        commands = mod.change()
        ranges = for command <- commands, do: get_command_range(command)
        {vsn, ranges}
      end)

    {final_version, _} = Enum.max(new_migrations, &>=/2, fn -> {latest_active_version, nil} end)

    if final_version == latest_active_version do
      :ignore
    else
      {:ok, claim_key(), new_migrations,
       %State{repo: repo, final_version: final_version, limit: limit}}
    end
  end

  @impl true
  def done?(state, _tx) do
    active_versions = state.repo.all(SchemaMigration.versions())
    {state.final_version in active_versions, state}
  end

  @impl true
  def next(state, _tx, [{vsn, []} | new_migrations]) do
    {:ok, _} = SchemaMigration.up(state.repo, vsn)
    {[vsn], new_migrations, state}
  end

  def next(state, tx, [{vsn, [{command_kv, command_cursor} | commands]} | new_migrations]) do
    case exec_command_next(state, tx, command_kv, command_cursor) do
      {:more, command_cursor} ->
        {[], [{vsn, [{command_kv, command_cursor} | commands]} | new_migrations], state}

      {:done, _command_cursor} ->
        {_command, {inventory_key, idx}} = command_kv
        :erlfdb.set(tx, inventory_key, Pack.to_fdb_value(idx))
        {[], [{vsn, commands} | new_migrations], state}
    end
  end

  defp get_command_range(
         command =
           {:create,
            %Index{
              schema: schema,
              name: index_name,
              columns: index_fields,
              options: index_options
            }}
       ) do
    {inventory_key, idx} =
      IndexInventory.new_index(Schema.get_source(schema), index_name, index_fields, index_options)

    command_kv = {command, {inventory_key, idx}}
    {command_kv, Indexer.create_range(idx)}
  end

  defp exec_command_next(
         %State{limit: limit},
         tx,
         {_command = {:create, %Index{schema: schema}}, {_ck, idx}},
         command_cursor
       ) do
    case Indexer.create(tx, idx, schema, command_cursor, limit) do
      {^limit, next_command_cursor} ->
        {:more, next_command_cursor}

      {_, next_command_cursor} ->
        {:done, next_command_cursor}
    end
  end
end
