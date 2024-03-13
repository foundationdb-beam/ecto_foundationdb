defmodule Ecto.Adapters.FoundationDB.Migration.Runner do
  @moduledoc false

  require Logger

  alias Ecto.Adapters.FoundationDB.Exception.Unsupported
  alias Ecto.Adapters.FoundationDB.Migration.Index

  @doc """
  Runs the given migration.
  """
  def run(repo, _config, version, module, direction, operation, _migrator_direction, opts) do
    level = Keyword.get(opts, :log, :info)
    log_adapter = Keyword.get(opts, :log_adapter, false)
    log = %{level: level, log_adapter: log_adapter}

    log(level, "== Running #{version} #{inspect(module)}.#{operation}/0 #{direction}")

    {time, _} =
      :timer.tc(fn -> perform_operation(repo, module, direction, log, operation) end)

    log(level, "== Migrated #{version} in #{inspect(div(time, 100_000) / 10)}s")
  end

  @doc """
  Executes queue migration commands.

  Reverses the order commands are executed when doing a rollback
  on a change/0 function and resets commands queue.
  """

  def perform_operation(repo, module, direction, log, operation) when is_atom(operation) do
    result = apply(module, operation, [])
    false = is_atom(result)
    perform_operation(repo, module, direction, log, result)
  end

  def perform_operation(repo, migration, direction, log, commands) when is_list(commands) do
    commands = if direction == :backward, do: commands, else: Enum.reverse(commands)

    for command <- commands do
      execute_in_direction(repo, migration, direction, log, command)
    end
  end

  def perform_operation(_repo, migration, _direction, _log, command) do
    raise Unsupported, """
    An Ecto FoundationDB migration must specify a list of migration commands.

    Your function in #{migration} returned #{inspect(command)}
    """
  end

  ## Execute

  defp execute_in_direction(repo, migration, :forward, log, command) do
    log_and_execute_ddl(repo, migration, log, command)
  end

  defp execute_in_direction(repo, migration, :backward, log, command) do
    if reversed = reverse(command) do
      log_and_execute_ddl(repo, migration, log, reversed)
    else
      raise Ecto.MigrationError,
        message:
          "cannot reverse migration command: #{inspect(command)}. " <>
            "You will need to explicitly define up/0 and down/0 in your migration"
    end
  end

  defp reverse({:create, %Index{} = index}),
    do: {:drop, index, :restrict}

  defp reverse({:create_if_not_exists, %Index{} = index}),
    do: {:drop_if_exists, index, :restrict}

  defp reverse({:drop, %Index{} = index, _}),
    do: {:create, index}

  defp reverse({:drop_if_exists, %Index{} = index, _}),
    do: {:create_if_not_exists, index}

  defp reverse({:rename, %Index{} = index, new_name}),
    do: {:rename, %{index | name: new_name}, index.name}

  defp reverse(_command), do: false

  ## Helpers

  defp log_and_execute_ddl(repo, _migration, log, {instruction, %Index{} = index}) do
    log_and_execute_ddl(repo, log, {instruction, index})
  end

  defp log_and_execute_ddl(repo, _migration, log, command) do
    log_and_execute_ddl(repo, log, command)
  end

  defp log_and_execute_ddl(_repo, _log, func) when is_function(func, 0) do
    func.()
    :ok
  end

  defp log_and_execute_ddl(repo, %{level: level, log_adapter: log_adapter}, command) do
    log(level, inspect(command))
    meta = Ecto.Adapter.lookup_meta(repo)

    {:ok, logs} =
      repo.__adapter__().execute_ddl(meta, command, timeout: :infinity, log: log_adapter)

    Enum.each(logs, fn {ddl_log_level, message, metadata} ->
      ddl_log(ddl_log_level, level, message, metadata)
    end)

    :ok
  end

  defp ddl_log(_level, false, _msg, _metadata), do: :ok
  defp ddl_log(level, _, msg, metadata), do: log(level, msg, metadata)

  defp log(level, msg, metadata \\ [])
  defp log(false, _msg, _metadata), do: :ok
  defp log(true, msg, metadata), do: Logger.log(:info, msg, metadata)
  defp log(level, msg, metadata), do: Logger.log(level, msg, metadata)
end
