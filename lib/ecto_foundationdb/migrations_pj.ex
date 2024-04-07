defmodule EctoFoundationDB.MigrationsPJ do
  @moduledoc false
  alias EctoFoundationDB.Indexer
  alias EctoFoundationDB.Layer.IndexInventory
  alias EctoFoundationDB.Layer.Pack
  alias EctoFoundationDB.Migration.Index
  alias EctoFoundationDB.Migration.SchemaMigration
  alias EctoFoundationDB.ProgressiveJob
  alias EctoFoundationDB.Schema

  @behaviour EctoFoundationDB.ProgressiveJob

  defmodule State do
    @moduledoc false
    defstruct [:repo, :final_version, :limit]
  end

  alias __MODULE__.State

  def claim_keys(new_migrations) do
    sources = get_sources(new_migrations, [])
    for source <- sources, do: claim_key(source)
  end

  def claim_key(source),
    do: Pack.namespaced_pack(SchemaMigration.source(), "claim", ["#{source}"])

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
      {:ok, claim_keys(new_migrations), new_migrations,
       %State{repo: repo, final_version: final_version, limit: limit}}
    end
  end

  @impl true
  def done?(state, _tx) do
    active_versions = state.repo.all(SchemaMigration.versions())
    {state.final_version in active_versions, state}
  end

  @impl true
  def next(state, _tx, []) do
    {[], [], state}
  end

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
              concurrently: concurrently,
              options: index_options
            }}
       ) do
    index_options = Keyword.merge(index_options, concurrently: concurrently)

    {inventory_key, idx} =
      IndexInventory.new_index(Schema.get_source(schema), index_name, index_fields, index_options)

    command_kv = {command, {inventory_key, idx}}
    {start_key, end_key} = Indexer.create_range(idx)
    {command_kv, {start_key, start_key, end_key}}
  end

  defp exec_command_next(
         %State{limit: limit},
         tx,
         {_command = {:create, %Index{schema: schema}}, {_ck, idx}},
         {start_key, cursor_key, end_key}
       ) do
    case Indexer.create(tx, idx, schema, {cursor_key, end_key}, limit) do
      {^limit, {next_cursor_key, ^end_key}} ->
        {:more, {start_key, next_cursor_key, end_key}}

      {_, {next_cursor_key, ^end_key}} ->
        {:done, {start_key, next_cursor_key, end_key}}
    end
  end

  def get_sources([], acc) do
    acc
  end

  def get_sources([{_vsn, commands} | new_migrations], acc) do
    sources =
      for {command_kv, _} <- commands do
        {_command = {:create, %Index{}}, {_ck, idx}} = command_kv
        idx[:source]
      end

    get_sources(new_migrations, acc ++ sources)
  end

  def get_idx_being_created(
        {_claim_ref, [{_vsn, [{command_kv, command_cursor} | _commands]} | _new_migrations]}
      ) do
    {_command = {:create, %Index{}}, {_ck, idx}} = command_kv
    {start_key, cursor_key, end_key} = command_cursor
    {idx, {start_key, cursor_key, end_key}}
  end

  def get_idx_being_created(_) do
    nil
  end
end
