defmodule EctoFoundationDBProgressiveJobTest.TestJob do
  @moduledoc false
  alias Ecto.Adapters.FoundationDB

  alias EctoFoundationDB.Layer.Pack
  alias EctoFoundationDB.ProgressiveJob

  @behaviour EctoFoundationDB.ProgressiveJob

  @done_key "test_job_done"
  @claim_key "test_job"
  @limit 100

  def reset(tenant) do
    FoundationDB.transactional(tenant, fn tx ->
      :erlfdb.clear_range(tx, "test_", "test~")
      :erlfdb.clear(tx, @done_key)
      :erlfdb.clear(tx, @claim_key)
    end)
  end

  def transactional(tenant, args) do
    set_process_config()

    ProgressiveJob.new(tenant, __MODULE__, args)
    |> ProgressiveJob.transactional_stream()
    |> Enum.to_list()
  end

  defp set_process_config() do
    Process.put(:claim_stale_msec, 100)
    Process.put(:claim_watch_timeout, 120)
  end

  @impl true
  def init(args) do
    state = args |> Map.put(:count, 0)
    {:ok, [@claim_key], {"", "\xFF"}, state}
  end

  @impl true
  def done?(state, tx) do
    val =
      tx
      |> :erlfdb.get(@done_key)
      |> :erlfdb.wait()

    {val != :not_found, state}
  end

  @impl true
  def next(state = %{count: count}, tx, {start_key, end_key}) do
    if count > state.throw_after, do: throw(:test_error)

    kvs =
      tx
      |> :erlfdb.get_range(start_key, end_key, limit: @limit)
      |> :erlfdb.wait()

    # Pretend that we're writing to a bunch of keys, as we would be doing for index creation
    Enum.each(kvs, fn {k, _} -> :erlfdb.add_write_conflict_key(tx, "test_" <> k) end)

    emit =
      if length(kvs) < @limit do
        # raises exception if the key already exists, similiar to an `Ecto.Repo` insert
        :not_found = :erlfdb.wait(:erlfdb.get(tx, @done_key))
        :erlfdb.set(tx, @done_key, "done")
        [:done]
      else
        []
      end

    start_key =
      case Enum.reverse(kvs) do
        [{key, _} | _] ->
          key

        [] ->
          end_key
      end

    {emit, {start_key, end_key}, %{state | count: count + length(kvs)}}
  end
end

defmodule EctoFoundationDBProgressiveJobTest do
  alias EctoFoundationDBProgressiveJobTest.TestJob

  alias Ecto.Adapters.FoundationDB

  alias EctoFoundationDB.Layer.Pack

  use Ecto.Integration.MigrationsCase

  @n_seed 1000

  # When n_tasks == 1, the job is run normally, happy path
  # When n_tasks > 1, the running job fails halfway through, and another job (exactly one) takes over
  defp async_jobs(tenant, n_tasks, n_seed) do
    TestJob.reset(tenant)

    fun = fn
      id = 1 when n_tasks != 1 ->
        try do
          TestJob.transactional(tenant, %{id: id, throw_after: 100})
        catch
          :test_error ->
            :test_error
        end

      id ->
        # The sleep ensures that id=1 claims the work first
        :timer.sleep(20)
        TestJob.transactional(tenant, %{id: id, throw_after: n_seed + 1})
    end

    1..n_tasks
    |> Task.async_stream(fun,
      ordered: false,
      max_concurrency: System.schedulers_online() * 2,
      timeout: :infinity
    )
    |> Enum.to_list()
    |> Enum.filter(fn
      {:ok, :test_error} -> false
      {:ok, []} -> false
      _ -> true
    end)
  end

  test "progressive job contract", context do
    tenant = context[:tenant]

    FoundationDB.transactional(tenant, fn tx ->
      for i <- 1..@n_seed, do: :erlfdb.set(tx, Ecto.UUID.generate(), Pack.to_fdb_value(i))
    end)

    assert [{:ok, [:done]}] == async_jobs(tenant, 1, @n_seed)
    assert [{:ok, [:done]}] == async_jobs(tenant, 20, @n_seed)
  end
end
