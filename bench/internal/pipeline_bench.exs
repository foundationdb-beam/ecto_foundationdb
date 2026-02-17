# Pipeline Benchmark
#
# Demonstrates the performance advantage of EctoFoundationDB's async pipelining.
#
# In a traditional database adapter, N queries require N sequential round-trips.
# EctoFDB's async_* functions allow issuing N queries within a transaction and
# resolving them all in a single pipelined round-trip via Repo.await/1.
#
# Run: MIX_ENV=bench mix run bench/pipeline_bench.exs

alias Ecto.Adapters.FoundationDB
alias EctoFoundationDB.Tenant

# -- Schema, Migration, Repo (defined before cluster start) -------------------

defmodule Bench.User do
  use Ecto.Schema

  @primary_key {:id, :binary_id, autogenerate: true}

  schema "users" do
    field(:name, :string)
    field(:notes, :string)
  end
end

defmodule Bench.Migration do
  use EctoFoundationDB.Migration
  def change, do: [create(index(Bench.User, [:name]))]
end

defmodule Bench.Repo do
  use Ecto.Repo, otp_app: :ecto_foundationdb, adapter: Ecto.Adapters.FoundationDB
  use EctoFoundationDB.Migrator
  def migrations, do: [{0, Bench.Migration}]
end

# -- Start cluster and Repo ---------------------------------------------------

Code.require_file("bench/cluster.exs")
cluster_file = Bench.Cluster.start()

Application.put_env(:ecto_foundationdb, Bench.Repo,
  cluster_file: cluster_file,
  storage_id: EctoFoundationDB.Bench
)

{:ok, _} = FoundationDB.ensure_all_started(Bench.Repo.config(), :temporary)
{:ok, _} = Bench.Repo.start_link(log: false)

tenant = Tenant.open!(Bench.Repo, "bench-pipeline")

# -- Seed data ----------------------------------------------------------------

names =
  for i <- 1..100 do
    "user_#{String.pad_leading("#{i}", 4, "0")}"
  end

IO.puts("Seeding #{length(names)} users...")

Bench.Repo.transactional(tenant, fn ->
  Enum.each(names, fn name ->
    Bench.Repo.insert!(struct(Bench.User, name: name))
  end)
end)

IO.puts("Seeding complete.\n")

# -- Benchmark ----------------------------------------------------------------

# Pick random subsets of names to query
pick = fn n -> Enum.take_random(names, n) end

Benchee.run(
  %{
    "sequential get_by (1 query)" => fn ->
      [name] = pick.(1)
      Bench.Repo.get_by(Bench.User, [name: name], prefix: tenant)
    end,
    "pipelined get_by (1 query)" => fn ->
      [name] = pick.(1)

      Bench.Repo.transactional(tenant, fn ->
        f = Bench.Repo.async_get_by(Bench.User, name: name)
        Bench.Repo.await([f])
      end)
    end,
    "sequential get_by (10 queries)" => fn ->
      query_names = pick.(10)

      Bench.Repo.transactional(tenant, fn ->
        Enum.map(query_names, fn name ->
          Bench.Repo.get_by(Bench.User, name: name)
        end)
      end)
    end,
    "pipelined get_by (10 queries)" => fn ->
      query_names = pick.(10)

      Bench.Repo.transactional(tenant, fn ->
        futures = Enum.map(query_names, &Bench.Repo.async_get_by(Bench.User, name: &1))
        Bench.Repo.await(futures)
      end)
    end,
    "sequential get_by (50 queries)" => fn ->
      query_names = pick.(50)

      Bench.Repo.transactional(tenant, fn ->
        Enum.map(query_names, fn name ->
          Bench.Repo.get_by(Bench.User, name: name)
        end)
      end)
    end,
    "pipelined get_by (50 queries)" => fn ->
      query_names = pick.(50)

      Bench.Repo.transactional(tenant, fn ->
        futures = Enum.map(query_names, &Bench.Repo.async_get_by(Bench.User, name: &1))
        Bench.Repo.await(futures)
      end)
    end
  },
  time: 10,
  warmup: 2,
  formatters: [Benchee.Formatters.Console]
)

# -- Cleanup -------------------------------------------------------------------

Bench.Repo.delete_all(Bench.User, prefix: tenant)
Tenant.clear_delete!(Bench.Repo, "bench-pipeline")
Bench.Cluster.stop()
