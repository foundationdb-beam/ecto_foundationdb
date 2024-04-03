defmodule Ecto.Adapters.FoundationDB.ProgressiveJob do
  @moduledoc """
  A `ProgressiveJob` is a transaction that can be executed in multiple parts, as long
  as those parts have a well defined progression. For example, a job might
  endeavor to process a range of keys with a corresponding internal limit.

  ## Behaviour

  To implement a `ProgressiveJob`, you must define a module that implements the
  `ProgressiveJob` beahviour.

  - `init/1` - Initializes the job state. This function is called once at the beginning
    of the job.

    The input term is the `init_args` from `new/3`.

    It should return a tuple with the following elements:

    `{:ok, claim_key, cursor, state}`

    The `claim_key` is a key that will be written to FDB and used by `ProgressiveJob` to maintain
    transactional isolation. It will be cleared after the successful execution of the job.

    The `cursor` terms define the range of terms that will be processed. Each iteration of the job
    must change the cursor to ensure that observers of the job can see progress.

    The `state` is a term that is passed to each callback.

  - `done?/2` - Determines if the job is complete. This function is called multiple times as
    necessary to maintain tranactional isolation. It's critical that the job reads a permanent
    key from the databases to determine if the job is complete.

    The input terms are the `state` and the `:erlfdb.transaction()`.

    It should return a tuple with the following elements:

    `{done?, state}`

    The `done?` is a boolean that indicates if the job is complete. If `true`, the job will stop
    executing.

    The `state` is a term that is passed to each callback.

  - `next/3` - Processes the next batch of work.

    The input terms are the `state`, the `:erlfdb.transaction()`, and the `cursor`.

    It should return a tuple with the following elements:

    `{emit, cursor, state}`

    `emit` is either an empty list, or a list of values to be emitted on the `Stream`. The `ProgressiveJob`
    does not inspect this term. It's passed directly to the `Stream` implementation.

    The `cursor` defines the updated cursor for the next iteration of the job. It's important that this
    term changes on each successful iteration. Other job executions watch the cursor to determine if the
    job is making progress.

    The `state` is a term that is passed to each callback.
  """
  alias Ecto.Adapters.FoundationDB
  alias Ecto.Adapters.FoundationDB.Layer.Pack

  @callback init(term()) ::
              {:ok, :erlfdb.key(), term(), term()} | :ignore
  @callback done?(term(), :erlfdb.transaction()) :: {boolean(), term()}

  @callback next(term(), :erlfdb.transaction(), term()) ::
              {list(), {:erlfdb.key(), :erlfdb.key()}, term()}

  @claim_stale_msec 5100
  @claim_watch_timeout 5120

  defstruct tenant: nil,
            ref: nil,
            claim_key: nil,
            cursor: nil,
            module: nil,
            init_args: nil,
            state: nil,
            done?: false,
            last_claimed_by: nil,
            claim_updated_at: nil

  @doc """
  Creates a ProgressiveJob struct to be used in `transactional_stream/1`.

  ## Parameters

    * `tenant` - The tenant to run the job for.
    * `module` - The module implementing the `ProgressiveJob` behaviour.
    * `init_args` - The arguments to pass to the `init/1` callback.
  """
  def new(tenant, module, init_args) do
    %__MODULE__{
      tenant: tenant,
      ref: make_ref(),
      module: module,
      init_args: init_args
    }
  end

  @doc """
  Creates a Stream that runs the job in a transactional context.

  The transactional context spans one or more FoundationDB transactions. This allows
  the job to take longer than the maximum 5 seconds.

  The behaviour module is in control of the values that get emitted for the stream.

  The stream will keep running until the behaviour module responds with `true` on the
  `done?/2` callback.

  ## Parameters

    * `job` - The `ProgressiveJob` struct created by `new/3`.
  """
  def transactional_stream(job = %__MODULE__{module: module, init_args: init_args}) do
    # For any future debuggers: Please remember that the function used in :erlfdb.transactional
    # can be executed and then rolled back, and then executed again. On the second execution, it
    # may follow a different code path. So, any log messages you add are not true evidence
    # of any writes being made to the database. In other words, logs are side effects.
    case module.init(init_args) do
      {:ok, claim_key, cursor, state} ->
        job = %__MODULE__{
          job
          | claim_key: claim_key,
            cursor: cursor,
            state: state
        }

        Stream.resource(fn -> tx_stream_start(job) end, &tx_stream_next/1, & &1)

      :ignore ->
        Stream.unfold(0, fn _ -> nil end)
    end
  end

  defp tx_stream_start(
         job = %__MODULE__{
           tenant: tenant,
           ref: ref,
           module: module,
           state: state
         }
       ) do
    FoundationDB.transactional(tenant, fn tx ->
      {done?, state} = module.done?(state, tx)

      {claimed?, claimed_by, cursor} = claimed?(job, tx)

      if claimed? do
        if done? do
          :erlfdb.clear(tx, job.claim_key)
        else
          :erlfdb.set(tx, job.claim_key, Pack.to_fdb_value({ref, cursor}))
        end
      end

      %__MODULE__{
        job
        | done?: done?,
          cursor: cursor,
          state: state,
          last_claimed_by: claimed_by,
          claim_updated_at: now()
      }
    end)
  end

  defp tx_stream_next(%__MODULE__{done?: true} = job), do: {:halt, job}

  defp tx_stream_next(job = %__MODULE__{tenant: tenant}) do
    case FoundationDB.transactional(tenant, &in_tx_stream_next(job, &1)) do
      {after_tx, emit, job} ->
        after_tx.()
        {emit, job}

      {emit, job} ->
        {emit, job}
    end
  end

  defp in_tx_stream_next(job = %__MODULE__{module: module, state: state}, tx) do
    {done?, state} = module.done?(state, tx)

    if done? do
      {:halt, %__MODULE__{job | done?: true, state: state}}
    else
      in_tx_stream_next_exec(job, tx)
    end
  end

  defp in_tx_stream_next_exec(job = %__MODULE__{module: module, state: state}, tx) do
    now = now()
    {claimed?, claimed_by, cursor} = claimed?(job, tx)

    {emit, cursor, state} =
      if claimed?, do: module.next(state, tx, cursor), else: {[], cursor, state}

    {done?, state} = module.done?(state, tx)

    job = %__MODULE__{
      job
      | done?: done?,
        cursor: cursor,
        state: state,
        last_claimed_by: claimed_by,
        claim_updated_at: get_claim_updated_at(job, claimed_by, cursor, now)
    }

    in_tx_stream_finish_iteration(
      {emit, job},
      tx,
      now,
      claimed?,
      done?,
      now - job.claim_updated_at > claim_stale_msec()
    )
  end

  # claimed? and done?
  defp in_tx_stream_finish_iteration({emit, job}, tx, _now, true, true, _) do
    :erlfdb.clear(tx, job.claim_key)
    {emit, job}
  end

  # claimed? and not done?
  defp in_tx_stream_finish_iteration({emit, job}, tx, _now, true, false, _) do
    :erlfdb.set(tx, job.claim_key, Pack.to_fdb_value({job.ref, job.cursor}))
    {emit, job}
  end

  # not claimed? and done?
  defp in_tx_stream_finish_iteration({emit, job}, _tx, _now, false, true, _) do
    {emit, job}
  end

  # not claimed? and not done? and claim_age > claim_stale_msec()
  defp in_tx_stream_finish_iteration({_emit, job}, tx, now, false, false, true) do
    :erlfdb.set(tx, job.claim_key, Pack.to_fdb_value({job.ref, job.cursor}))
    in_tx_stream_next(%__MODULE__{job | claim_updated_at: now}, tx)
  end

  # not claimed? and not done? and claim_age <= claim_stale_msec()
  defp in_tx_stream_finish_iteration({emit, job}, tx, _now, false, false, false) do
    # Using a watch avoids busy-looping on workers that do not have a claim
    wclaim = :erlfdb.watch(tx, job.claim_key)
    after_tx = fn -> await_watched_claim(wclaim) end
    {after_tx, emit, job}
  end

  defp await_watched_claim(wclaim) do
    try do
      :erlfdb.wait(wclaim, timeout: claim_watch_timeout())
    catch
      :error, {:timeout, _} ->
        :ok
    end
  end

  defp claimed?(%__MODULE__{ref: ref, claim_key: claim_key, cursor: cursor}, tx) do
    res = :erlfdb.wait(:erlfdb.get(tx, claim_key))

    if res == :not_found do
      {true, nil, cursor}
    else
      {claim_ref, claim_cursor} = Pack.from_fdb_value(res)
      {claim_ref == ref, claim_ref, claim_cursor}
    end
  end

  defp get_claim_updated_at(
         %__MODULE__{
           cursor: old_cursor,
           last_claimed_by: old_claimed_by,
           claim_updated_at: old_claim_updated_at
         },
         new_claimed_by,
         new_cursor,
         _now
       )
       when old_claimed_by == new_claimed_by and old_cursor == new_cursor do
    old_claim_updated_at
  end

  defp get_claim_updated_at(_job, _new_claimed_by, _new_cursor, now) do
    now
  end

  defp now(), do: :erlang.monotonic_time(:millisecond)

  defp claim_stale_msec() do
    case Process.get(:claim_stale_msec) do
      nil -> @claim_stale_msec
      x -> x
    end
  end

  defp claim_watch_timeout() do
    case Process.get(:claim_watch_timeout) do
      nil -> @claim_watch_timeout
      x -> x
    end
  end
end
