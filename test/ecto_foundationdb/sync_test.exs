defmodule EctoFoundationDBSyncTest do
  use ExUnit.Case, async: true
  alias EctoFoundationDB.Sync

  describe "assign_map/3 regular" do
    test "assigns a value to a key in the assigns map" do
      assert %{a: 1} = Sync.assign_map(%{}, a: 1)
    end

    test "overrides a value in the assigns map" do
      assert %{a: 1} = Sync.assign_map(%{a: 0}, a: 1)
    end

    test "replaces maps without merging" do
      assert %{a: a} = Sync.assign_map(%{a: %{x: 0}}, a: %{y: 1})
      assert [:y] = Map.keys(a)
    end
  end

  describe "assign_map/3 nested" do
    test "assigns a value to a key in the assigns map" do
      assert %{a: %{b: 1}} = Sync.assign_map(%{}, [{[:a, :b], 1}])
    end

    test "overrides a value in the assigns map" do
      assert %{a: %{b: 1}} = Sync.assign_map(%{}, [{[:a, :b], 1}])
    end

    test "merges maps" do
      assert %{a: %{x: 0, y: 1}} = Sync.assign_map(%{a: %{x: 0}}, [{[:a, :y], 1}])
    end
  end
end
