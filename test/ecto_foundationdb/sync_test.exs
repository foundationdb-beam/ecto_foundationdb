defmodule EctoFoundationDBSyncTest do
  use ExUnit.Case, async: true
  alias EctoFoundationDB.Sync

  alias EctoFoundationDB.Sync.IdList

  describe "assign_map/3 std" do
    test "assigns a value to a key in the assigns map" do
      assert %{a: 1} = Sync.assign_map(%{}, [a: 1], [])
    end

    test "overrides a value in the assigns map" do
      assert %{a: 1} = Sync.assign_map(%{a: 0}, [a: 1], [])
    end

    test "replaces maps without merging" do
      assert %{a: a} = Sync.assign_map(%{a: %{x: 0}}, [a: %{y: 1}], [])
      assert [:y] = Map.keys(a)
    end
  end

  describe "assign_map/3 list with ids" do
    test "assigns a list" do
      assert %{a: [%{id: 1, v: 2}]} =
               Sync.assign_map(%{}, [], a: IdList.Markup.new([%{id: 1, v: 2}]))

      assert %{a: [%{id: 1, v: 2}]} =
               Sync.assign_map(
                 %{a: [%{id: 0, v: 0}]},
                 [],
                 [a: IdList.Markup.new([%{id: 1, v: 2}])],
                 idlist: :replace
               )
    end

    test "appends to list" do
      assert %{a: [%{id: 0, v: 0}, %{id: 1, v: 2}]} =
               Sync.assign_map(%{a: [%{id: 0, v: 0}]}, [], a: IdList.Markup.new([%{id: 1, v: 2}]))
    end

    test "replaces in list" do
      assert %{a: [%{id: 1, v: 2}, %{id: 2, v: 0}]} =
               Sync.assign_map(%{a: [%{id: 1, v: 0}, %{id: 2, v: 0}]}, [],
                 a: IdList.Markup.new([%{id: 1, v: 2}])
               )
    end
  end
end
