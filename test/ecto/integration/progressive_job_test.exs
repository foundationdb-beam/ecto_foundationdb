defmodule EctoFoundationDBProgressiveJobTest.TestJob do
  @moduledoc false
  alias Ecto.Adapters.FoundationDB

  alias EctoFoundationDB.Layer.Pack
  alias EctoFoundationDB.ProgressiveJob
  alias EctoFoundationDB.Tenant

  @behaviour EctoFoundationDB.ProgressiveJob

  @limit 100

  defp done_key(tenant), do: Tenant.pack(tenant, {"test_job_done"})
  defp claim_key(tenant), do: Tenant.pack(tenant, {"test_job"})
  defp range(tenant), do: Tenant.range(tenant, {"test_"})

  def reset(tenant) do
    FoundationDB.transactional(tenant, fn tx ->
      {sk, ek} = range(tenant)
      :erlfdb.clear_range(tx, sk, ek)
      :erlfdb.clear(tx, done_key(tenant))
      :erlfdb.clear(tx, claim_key(tenant))
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
  def init(tenant, args) do
    state = args |> Map.put(:count, 0) |> Map.put(:tenant, tenant)
    {sk, ek} = Tenant.range(tenant, {})
    {:ok, [claim_key(tenant)], {sk, ek}, state}
  end

  @impl true
  def done?(state, tx) do
    val =
      tx
      |> :erlfdb.get(done_key(state.tenant))
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
    Enum.each(kvs, fn {k, _} ->
      :erlfdb.add_write_conflict_key(tx, Tenant.pack(state.tenant, {"test_" <> k}))
    end)

    emit =
      if length(kvs) < @limit do
        # raises exception if the key already exists, similar to an `Ecto.Repo` insert
        :not_found = :erlfdb.wait(:erlfdb.get(tx, done_key(state.tenant)))
        :erlfdb.set(tx, done_key(state.tenant), "done")
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
  alias EctoFoundationDB.Tenant

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
      key = Tenant.pack(tenant, {Ecto.UUID.generate()})
      for i <- 1..@n_seed, do: :erlfdb.set(tx, key, Pack.to_fdb_value(i))
    end)

    assert [{:ok, [:done]}] == async_jobs(tenant, 1, @n_seed)
    assert [{:ok, [:done]}] == async_jobs(tenant, 20, @n_seed)
  end
end
