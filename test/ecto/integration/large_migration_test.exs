defmodule Ecto.Integration.LargeMigrationTest do
  use Ecto.Integration.MigrationsCase, async: false

  alias Ecto.Integration.TestRepo

  alias EctoFoundationDB.Schemas.User
  alias Ecto.Adapters.FoundationDB.Tenant
  alias EctoFoundationDB.Integration.TestMigrator

  @chunk_size 100 #2000
  @num_chunks 100 #200
  @num_open 10

  test "large migration", context do
    tenant_id = context[:tenant_id]
    tenant = context[:tenant]

    inserter = fn _j ->
      {:ok, _user1} = TestRepo.insert(%User{name: Ecto.UUID.generate()})
    end

    chunk_inserter = fn _i ->
      TestRepo.transaction(
        fn ->
          for j <- 1..@chunk_size, do: inserter.(j)
        end,
        prefix: tenant
      )
    end

    max_concurrency = System.schedulers_online() * 2

    stream =
      Task.async_stream(1..@num_chunks, chunk_inserter,
        ordered: false,
        max_concurrency: max_concurrency,
        timeout: :infinity
      )

    Stream.run(stream)

    stream =
      Task.async_stream(
        1..@num_open,
        fn _ ->
          Tenant.open(TestRepo, tenant_id, migrator: TestMigrator)
        end,
        ordered: false,
        max_concurrency: max_concurrency,
        timeout: :infinity
      )

    Stream.run(stream)
  end
end
