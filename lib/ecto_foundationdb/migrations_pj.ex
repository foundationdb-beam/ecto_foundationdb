defmodule EctoFoundationDB.MigrationsPJ do
  @moduledoc false
  alias EctoFoundationDB.Future
  alias EctoFoundationDB.Indexer
  alias EctoFoundationDB.Layer.Metadata
  alias EctoFoundationDB.Layer.MetadataVersion
  alias EctoFoundationDB.Layer.Pack
  alias EctoFoundationDB.Migration.Index
  alias EctoFoundationDB.Migration.SchemaMigration
  alias EctoFoundationDB.Options
  alias EctoFoundationDB.ProgressiveJob
  alias EctoFoundationDB.Schema
  alias EctoFoundationDB.Tenant

  require Logger

  @behaviour EctoFoundationDB.ProgressiveJob

  defmodule State do
    @moduledoc false
    defstruct [:tenant, :repo, :final_version, :limit, :options]
  end

  alias __MODULE__.State

  def claim_keys(tenant, new_migrations) do
    sources = get_sources(new_migrations, [])
    for source <- sources, do: claim_key(tenant, source)
  end

  def tx_get_claim_status(tenant, tx, source, future) do
    Future.set(
      future,
      tx,
      :erlfdb.get(tx, claim_key(tenant, source)),
      &decode_claim_status(&1, source)
    )
  end

  def tx_add_claim_write_conflict(tenant, tx, source) do
    :erlfdb.add_write_conflict_key(tx, claim_key(tenant, source))
  end

  def claim_key(tenant, source),
    do: Pack.namespaced_pack(tenant, SchemaMigration.source(), "claim", ["#{source}"])

  def transactional(repo, tenant, migrator, limit, options) do
    active_versions = repo.all(SchemaMigration.versions(), prefix: tenant)

    tenant = Tenant.set_metadata_cache(tenant, :disabled)

    ProgressiveJob.new(tenant, __MODULE__, %{
      active_versions: active_versions,
      repo: repo,
      migrator: migrator,
      limit: limit,
      options: options
    })
    |> ProgressiveJob.transactional_stream()
    |> Enum.to_list()
  end

  @impl true
  def init(tenant, args) do
    %{
      active_versions: active_versions,
      repo: repo,
      migrator: migrator,
      limit: limit,
      options: options
    } = args

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
        ranges = for command <- commands, do: get_command_range(tenant, command)
        {vsn, ranges}
      end)

    {final_version, _} = Enum.max(new_migrations, &>=/2, fn -> {latest_active_version, nil} end)

    if final_version == latest_active_version do
      :ignore
    else
      {:ok, claim_keys(tenant, new_migrations), new_migrations,
       %State{
         tenant: tenant,
         repo: repo,
         final_version: final_version,
         limit: limit,
         options: options
       }}
    end
  end

  @impl true
  def done?(state, _tx) do
    active_versions = state.repo.all(SchemaMigration.versions())
    {state.final_version in active_versions, state}
  end

  @impl true
  def next(state, _tx, []) do
    after_tx = fn -> :ok end
    {after_tx, [], [], state}
  end

  def next(state, _tx, [{vsn, []} | new_migrations]) do
    {:ok, _} = SchemaMigration.up(state.repo, vsn)
    after_tx = fn -> :ok end
    {after_tx, [vsn], new_migrations, state}
  end

  def next(state, tx, [{vsn, [{command_kv, command_cursor} | commands]} | new_migrations]) do
    case exec_command_next(state, tx, command_kv, command_cursor) do
      {:more, command_cursor} ->
        after_tx = fn -> :ok end
        {after_tx, [], [{vsn, [{command_kv, command_cursor} | commands]} | new_migrations], state}

      :done ->
        after_tx = fn ->
          log_finished(state, vsn, command_kv)
        end

        {after_tx, [], [{vsn, commands} | new_migrations], state}
    end
  end

  defp get_command_range(tenant, command = {:create, index}) do
    {metadata_key, idx} = new_metadata_index(tenant, index)

    command_kv = {command, {metadata_key, idx}}
    {start_key, end_key} = Indexer.create_range(tenant, idx)
    {command_kv, {start_key, start_key, end_key}}
  end

  defp get_command_range(tenant, command = {:drop, index}) do
    {metadata_key, idx} = new_metadata_index(tenant, index)

    command_kv = {command, {metadata_key, idx}}
    drop_ranges = Indexer.drop_ranges(tenant, idx)
    {command_kv, drop_ranges}
  end

  defp exec_command_next(
         state,
         tx,
         {_command = {:create, index}, {metadata_key, idx}},
         cursor_key_tuple
       ) do
    %State{tenant: tenant, limit: limit} = state
    %Index{schema: schema} = index
    {start_key, cursor_key, end_key} = cursor_key_tuple

    # Each step in the job must update the global metadataVersion so that the
    # Metadata module will always read the most up-to-date partial_idxs
    MetadataVersion.tx_set_global(tx)

    case Indexer.create(tenant, tx, idx, schema, {cursor_key, end_key}, limit) do
      {_, {^cursor_key, ^end_key}} ->
        raise """
        Indexer did not make progress. Is the limit too small?

        tenant: #{inspect(tenant)}
        idx: #{inspect(idx)}
        schema: #{inspect(schema)}
        cursor_key: #{inspect(cursor_key, binaries: :as_strings)}
        end_key: #{inspect(end_key, binaries: :as_strings)}
        limit: #{inspect(limit)}
        """

      {^limit, {next_cursor_key, ^end_key}} ->
        {:more, {start_key, next_cursor_key, end_key}}

      {_, {_next_cursor_key, ^end_key}} ->
        :erlfdb.set(tx, metadata_key, Pack.to_fdb_value(idx))
        :done
    end
  end

  defp exec_command_next(
         _state,
         tx,
         {_command = {:drop, _index}, {metadata_key, _idx}},
         drop_ranges
       ) do
    Enum.each(drop_ranges, fn
      {start_key, end_key} ->
        :erlfdb.clear_range(tx, start_key, end_key)

      key ->
        :erlfdb.clear(tx, key)
    end)

    :erlfdb.clear(tx, metadata_key)

    :done
  end

  def get_sources([], acc) do
    acc
  end

  def get_sources([{_vsn, commands} | new_migrations], acc) do
    sources =
      for {command_kv, _} <- commands do
        {_command = {_create_or_drop, %Index{}}, {_ck, idx}} = command_kv
        idx[:source]
      end

    get_sources(new_migrations, acc ++ sources)
  end

  def get_idx_being_created({_claim_ref, []}) do
    nil
  end

  def get_idx_being_created(
        {_claim_ref,
         [
           {_vsn,
            [
              _command_kv = {{_command = {:create, %Index{}}, {_ck, idx}}, command_cursor}
              | _commands
            ]}
           | _new_migrations
         ]}
      ) do
    {start_key, cursor_key, end_key} = command_cursor
    {idx, {start_key, cursor_key, end_key}}
  end

  def get_idx_being_created({_claim_ref, [_not_a_create | new_migrations]}) do
    get_idx_being_created(new_migrations)
  end

  def get_idx_being_created(_) do
    nil
  end

  defp new_metadata_index(tenant, index) do
    %Index{
      schema: schema,
      name: index_name,
      columns: index_fields,
      concurrently: concurrently,
      options: index_options
    } = index

    index_options = Keyword.merge(index_options, concurrently: concurrently)

    Metadata.new_index(
      tenant,
      Schema.get_source(schema),
      index_name,
      index_fields,
      index_options
    )
  end

  defp log_finished(state, vsn, {_command = {:create, %Index{}}, {_ck, idx}}) do
    %State{tenant: tenant, options: options} = state

    if Options.get(options, :log) do
      Logger.notice(
        "[EctoFoundationDB] #{tenant.id} (#{vsn}) #{idx[:source]} => #{inspect(idx[:id])}"
      )
    end
  end

  defp log_finished(state, vsn, {_command = {:drop, %Index{}}, {_ck, idx}}) do
    %State{tenant: tenant, options: options} = state

    if Options.get(options, :log) do
      Logger.notice(
        "[EctoFoundationDB] #{tenant.id} (#{vsn}) #{idx[:source]} X #{inspect(idx[:id])}"
      )
    end
  end

  defp decode_claim_status(:not_found, _), do: {false, []}

  defp decode_claim_status(claim, source) do
    idx_being_created =
      Pack.from_fdb_value(claim)
      |> get_idx_being_created()

    partial_idxs =
      case idx_being_created do
        nil ->
          []

        p_idx = {idx, _} ->
          if idx[:source] == source do
            [p_idx]
          else
            []
          end
      end

    claim_active? = length(partial_idxs) > 0

    partial_idxs =
      Enum.filter(partial_idxs, fn {idx, _} ->
        p_options = idx[:options]
        Keyword.get(p_options, :concurrently, true)
      end)

    {claim_active?, partial_idxs}
  end
end
