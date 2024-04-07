defmodule Ecto.Integration.IndexerTest do
  use Ecto.Integration.MigrationsCase, async: true

  alias Ecto.Adapters.FoundationDB

  alias EctoFoundationDB.Indexer
  alias EctoFoundationDB.Layer.Pack
  alias EctoFoundationDB.Migrator

  alias Ecto.Integration.TestRepo

  alias EctoFoundationDB.Schemas.User

  alias Ecto.Integration.IndexerTest.NameStartsWithJ
  alias Ecto.Integration.IndexerTest.TestMigration
  alias Ecto.Integration.IndexerTest.TestMigrator

  defmodule TestMigrator do
    @moduledoc false

    use EctoFoundationDB.Migrator

    def migrations(), do: [{0, TestMigration}]
  end

  defmodule TestMigration do
    @moduledoc false
    use EctoFoundationDB.Migration

    def change() do
      [create(index(User, [:name], options: [indexer: NameStartsWithJ]))]
    end
  end

  defmodule NameStartsWithJ do
    @behaviour Indexer

    alias EctoFoundationDB.QueryPlan
    @count_key "name_starts_with_J_count"
    @index_key "name_starts_with_J_index/"

    # omitted for brevity. A complete Indexer must implement all functions
    @impl true
    def create_range(_idx), do: {"", "\xFF"}

    @impl true
    def create(_tx, _idx, _schema, _range, _limit), do: {0, {"\xFF", "\xFF"}}

    @impl true
    def clear(_tx, _idx, _schema, _kv), do: nil

    @impl true
    def set(tx, _idx, _schema, {_, data}) do
      name = Keyword.get(data, :name)

      if not is_nil(name) and String.starts_with?(name, "J") do
        :erlfdb.add(tx, @count_key, 1)

        # For simplicity, we duplicate the data into the index key
        :erlfdb.set(tx, @index_key <> data[:id], Pack.to_fdb_value(data))
      end
    end

    @impl true
    def range(_idx, plan, _options) do
      %QueryPlan{constraints: [%QueryPlan.Equal{field: :name, param: "J"}]} = plan
      {@index_key, :erlfdb_key.strinc(@index_key)}
    end

    def get_count(tenant) do
      FoundationDB.transactional(tenant, fn tx ->
        tx
        |> :erlfdb.get(@count_key)
        |> :erlfdb.wait()
        |> :binary.decode_unsigned(:little)
      end)
    end
  end

  @moduletag :integration
  describe "indexer" do
    test "name starts with J", context do
      tenant = context[:tenant]

      :ok = Migrator.up(TestRepo, tenant, migrator: TestMigrator)

      assert {:ok, _} = TestRepo.insert(%User{name: "Jesse"}, prefix: tenant)

      assert {:ok, _} = TestRepo.insert(%User{name: "Sarah"}, prefix: tenant)

      assert %User{name: "Jesse"} = TestRepo.get_by!(User, [name: "J"], prefix: tenant)

      assert 1 == NameStartsWithJ.get_count(tenant)
    end
  end
end
