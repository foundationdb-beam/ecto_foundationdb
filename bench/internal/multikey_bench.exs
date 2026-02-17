# Multikey Benchmark
#
# Measures the cost of EctoFoundationDB's transparent large-object encoding.
#
# FDB enforces a 100KB max value per key-value pair. EctoFDB automatically
# splits larger objects across multiple KV pairs with CRC32 integrity checks,
# and reassembles them on read — all invisible to the application.
#
# This benchmark compares read and write performance at different object sizes:
# single-key (1 KV pair), 2-chunk, and 5-chunk objects.
#
# Run: MIX_ENV=bench mix run bench/multikey_bench.exs

alias Ecto.Adapters.FoundationDB
alias EctoFoundationDB.Tenant

# -- Schema, Repo (defined before cluster start) ------------------------------

defmodule Bench.Blob do
  use Ecto.Schema

  @primary_key {:id, :binary_id, autogenerate: true}

  schema "blobs" do
    field(:data, :string)
  end
end

defmodule Bench.Repo do
  use Ecto.Repo, otp_app: :ecto_foundationdb, adapter: Ecto.Adapters.FoundationDB
  use EctoFoundationDB.Migrator
  def migrations, do: []
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

tenant = Tenant.open!(Bench.Repo, "bench-multikey")

# -- Seed data ----------------------------------------------------------------

# Generate deterministic payloads of specific sizes.
# The encoded Erlang term is slightly larger than the raw string, so we
# target the raw string size and let the encoding push it over the threshold.
make_payload = fn bytes -> :crypto.strong_rand_bytes(bytes) |> Base.encode64() |> binary_part(0, bytes) end

small_data  = make_payload.(10_000)    # ~10 KB  → 1 KV pair
medium_data = make_payload.(50_000)    # ~50 KB  → 1 KV pair
large_data  = make_payload.(150_000)   # ~150 KB → 2 KV pairs
xlarge_data = make_payload.(500_000)   # ~500 KB → 5-6 KV pairs

IO.puts("Seeding objects...")
IO.puts("  small:   #{byte_size(small_data)} bytes")
IO.puts("  medium:  #{byte_size(medium_data)} bytes")
IO.puts("  large:   #{byte_size(large_data)} bytes")
IO.puts("  xlarge:  #{byte_size(xlarge_data)} bytes")

small  = Bench.Repo.insert!(struct(Bench.Blob, data: small_data), prefix: tenant)
medium = Bench.Repo.insert!(struct(Bench.Blob, data: medium_data), prefix: tenant)
large  = Bench.Repo.insert!(struct(Bench.Blob, data: large_data), prefix: tenant)
xlarge = Bench.Repo.insert!(struct(Bench.Blob, data: xlarge_data), prefix: tenant)

IO.puts("Seeding complete.\n")

# -- Read Benchmark -----------------------------------------------------------

IO.puts("=== Read Benchmark ===\n")

Benchee.run(
  %{
    "read 10 KB (1 KV)" => fn ->
      Bench.Repo.get!(Bench.Blob, small.id, prefix: tenant)
    end,
    "read 50 KB (1 KV)" => fn ->
      Bench.Repo.get!(Bench.Blob, medium.id, prefix: tenant)
    end,
    "read 150 KB (2 KVs)" => fn ->
      Bench.Repo.get!(Bench.Blob, large.id, prefix: tenant)
    end,
    "read 500 KB (5+ KVs)" => fn ->
      Bench.Repo.get!(Bench.Blob, xlarge.id, prefix: tenant)
    end
  },
  time: 10,
  warmup: 2,
  formatters: [Benchee.Formatters.Console]
)

# -- Write Benchmark ----------------------------------------------------------

IO.puts("\n=== Write Benchmark ===\n")

Benchee.run(
  %{
    "write 10 KB (1 KV)" => fn ->
      Bench.Repo.insert!(struct(Bench.Blob, data: small_data), prefix: tenant)
    end,
    "write 50 KB (1 KV)" => fn ->
      Bench.Repo.insert!(struct(Bench.Blob, data: medium_data), prefix: tenant)
    end,
    "write 150 KB (2 KVs)" => fn ->
      Bench.Repo.insert!(struct(Bench.Blob, data: large_data), prefix: tenant)
    end,
    "write 500 KB (5+ KVs)" => fn ->
      Bench.Repo.insert!(struct(Bench.Blob, data: xlarge_data), prefix: tenant)
    end
  },
  time: 10,
  warmup: 2,
  formatters: [Benchee.Formatters.Console]
)

# -- Cleanup -------------------------------------------------------------------

Bench.Repo.delete_all(Bench.Blob, prefix: tenant)
Tenant.clear_delete!(Bench.Repo, "bench-multikey")
Bench.Cluster.stop()
