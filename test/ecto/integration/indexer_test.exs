defmodule Ecto.Integration.IndexerTest do
  use Ecto.Integration.MigrationsCase, async: true

  alias Ecto.Adapters.FoundationDB

  alias EctoFoundationDB.Indexer
  alias EctoFoundationDB.Layer.Pack
  alias EctoFoundationDB.Migrator

  alias Ecto.Integration.TestRepo

  alias EctoFoundationDB.Schemas.User

  alias Ecto.Integration.IndexerTest.NameStartsWithJ
  alias Ecto.Integration.IndexerTest.TestDropMigration
  alias Ecto.Integration.IndexerTest.TestMigration
  alias Ecto.Integration.IndexerTest.TestMigrator1
  alias Ecto.Integration.IndexerTest.TestMigrator2

  defmodule TestMigrator1 do
    @moduledoc false

    use EctoFoundationDB.Migrator

    def migrations(), do: [{0, TestMigration}]
  end

  defmodule TestMigrator2 do
    @moduledoc false

    use EctoFoundationDB.Migrator

    def migrations(), do: [{0, TestMigration}, {1, TestDropMigration}]
  end

  defmodule TestMigration do
    @moduledoc false
    use EctoFoundationDB.Migration

    def change() do
      [create(index(User, [:name], options: [indexer: NameStartsWithJ]))]
    end
  end

  defmodule TestDropMigration do
    @moduledoc false
    use EctoFoundationDB.Migration

    def change() do
      [drop(index(User, [:name], options: [indexer: NameStartsWithJ]))]
    end
  end

  defmodule NameStartsWithJ do
    @behaviour Indexer

    alias EctoFoundationDB.QueryPlan
    alias EctoFoundationDB.Tenant

    @count_key "name_starts_with_J_count"
    @index_key "name_starts_with_J_index/"

    # omitted for brevity. A complete Indexer must implement all functions
    @impl true
    def create_range(_tenant, _idx), do: {"", "\xFF"}

    @impl true
    def drop_ranges(tenant, _idx), do: [count_key(tenant), index_range(tenant)]

    @impl true
    def create(_tenant, _tx, _idx, _schema, _range, _limit), do: {0, {"\xFF", "\xFF"}}

    @impl true
    def clear(_tenant, _tx, _idx, _schema, _kv), do: nil

    @impl true
    def set(tenant, tx, _idx, _schema, {_, data}) do
      name = Keyword.get(data, :name)

      if not is_nil(name) and String.starts_with?(name, "J") do
        :erlfdb.add(tx, Tenant.pack(tenant, {@count_key}), 1)

        # For simplicity, we duplicate the data into the index key
        :erlfdb.set(tx, Tenant.pack(tenant, {@index_key, data[:id]}), Pack.to_fdb_value(data))
      end
    end

    @impl true
    def range(_idx, plan, _options) do
      %QueryPlan{constraints: [%QueryPlan.Equal{field: :name, param: "J"}]} = plan
      index_range(plan.tenant)
    end

    def get_count(tenant) do
      FoundationDB.transactional(tenant, fn tx ->
        tx
        |> :erlfdb.get(count_key(tenant))
        |> :erlfdb.wait()
        |> decode()
      end)
    end

    defp decode(:not_found), do: -1
    defp decode(x), do: :binary.decode_unsigned(x, :little)

    defp count_key(tenant) do
      Tenant.pack(tenant, {@count_key})
    end

    defp index_range(tenant) do
      Tenant.range(tenant, {@index_key})
    end
  end

  @moduletag :integration
  describe "indexer" do
    test ":create/drop: name starts with J", context do
      tenant = context[:tenant]

      :ok = Migrator.up(TestRepo, tenant, migrator: TestMigrator1, log: false)

      assert {:ok, _} = TestRepo.insert(%User{name: "Jesse"}, prefix: tenant)

      assert {:ok, _} = TestRepo.insert(%User{name: "Sarah"}, prefix: tenant)

      assert %User{name: "Jesse"} = TestRepo.get_by!(User, [name: "J"], prefix: tenant)

      assert 1 == NameStartsWithJ.get_count(tenant)

      :ok = Migrator.up(TestRepo, tenant, migrator: TestMigrator2, log: false)

      assert -1 == NameStartsWithJ.get_count(tenant)
    end
  end
end
