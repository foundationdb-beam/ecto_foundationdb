defmodule Ecto.Integration.LargeMigrationTest do
  use Ecto.Integration.MigrationsCase, async: false

  alias Ecto.Integration.TestRepo

  alias EctoFoundationDB.Tenant

  alias EctoFoundationDB.Integration.TestMigrator
  alias EctoFoundationDB.Schemas.User

  import Ecto.Query

  @chunk_size 100
  @num_chunks 20
  @migration_step 2

  defp do_insert(tenant, tag, i, j) do
    # user.id is chosen to distribute writes evenly among the keyspace during the migration

    i = i |> Integer.to_string() |> String.pad_leading(4, "0")
    j = j |> Integer.to_string() |> String.pad_leading(4, "0")

    {:ok, _user1} =
      TestRepo.insert(%User{id: "id-#{j}-#{i}-#{tag}", name: "name-#{j}-#{i}-#{tag}"},
        prefix: tenant
      )
  end

  defp do_insert_chunk(tenant, tag, i, sleep, reverse?) do
    seq = if reverse?, do: @chunk_size..1, else: 1..@chunk_size

    for j <- seq do
      if sleep > 0, do: :timer.sleep(sleep)
      tick = :erlang.monotonic_time(:microsecond)
      {:ok, user} = do_insert(tenant, tag, i, j)
      diff = :erlang.monotonic_time(:microsecond) - tick
      {diff, user}
    end
  end

  defp get_stream(fun) do
    Task.async_stream(1..@num_chunks, fun,
      ordered: false,
      max_concurrency: System.schedulers_online() * 2,
      timeout: :infinity
    )
  end

  defp seed_users(tenant) do
    insert_stream =
      get_stream(fn i ->
        TestRepo.transaction(
          fn ->
            {_, users} = Enum.unzip(do_insert_chunk(tenant, "user-a", i, 0, false))
            users
          end,
          prefix: tenant
        )
      end)

    insert_stream
    |> Stream.flat_map(fn {:ok, x} -> x end)
    |> Enum.to_list()
  end

  defp migrate_with_new_inserts(tenant_id, tenant) do
    open_tenant_stream =
      get_stream(fn i ->
        task = ins_task(tenant, i)

        # A short migration step ensures that there are many opportunities between
        # transactions
        Tenant.open(TestRepo, tenant_id, migrator: TestMigrator, migration_step: @migration_step)
        unless is_nil(task), do: Task.await(task)
      end)

    {ins_claim_time, ins_claim_users} =
      open_tenant_stream
      |> Stream.map(fn {:ok, x} -> x end)
      |> Stream.filter(fn
        nil -> false
        _ -> true
      end)
      |> Enum.to_list()
      |> List.flatten()
      |> Enum.unzip()

    ins_claim_time = Enum.max(ins_claim_time) / 1000.0
    ins_claim_users = List.flatten(ins_claim_users)
    {ins_claim_time, ins_claim_users}
  end

  defp ins_task(tenant, i) do
    # A short sleep in between each insert to distribute the writes over time
    # and inserting in reverse nearly guarantees that we will cross over the
    # advancing migration cursor
    ins_fun = fn i -> do_insert_chunk(tenant, "user-b", i, 2, true) end
    if i == 1, do: Task.async(fn -> ins_fun.(i) end)
  end

  test "large migration with concurrent writes", context do
    tenant_id = context[:tenant_id]
    tenant = context[:tenant]

    seed_users = seed_users(tenant)
    {_ins_claim_time, ins_claim_users} = migrate_with_new_inserts(tenant_id, tenant)

    all_users = seed_users ++ ins_claim_users

    assert((@num_chunks + 1) * @chunk_size == length(all_users))

    assert(length(all_users) == length(TestRepo.all(User, prefix: tenant)))

    assert(
      length(all_users) ==
        length(
          TestRepo.all(from(u in User, where: u.name > ^"\x00" and u.name < ^"\xF0"),
            prefix: tenant
          )
        )
    )
  end
end
