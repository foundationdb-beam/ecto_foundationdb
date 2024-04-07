defmodule EctoFoundationDBLayerIndexInventoryTest do
  use ExUnit.Case
  doctest EctoFoundationDB.Layer.IndexInventory

  alias EctoFoundationDB.Layer.IndexInventory
  alias EctoFoundationDB.QueryPlan.Between
  alias EctoFoundationDB.QueryPlan.Equal

  describe "select_index/2" do
    test "trival case" do
      idx_a = [id: :a, fields: [:user_id]]
      idx_b = [id: :b, fields: [:user_id]]
      constraints = [%Equal{field: :user_id}]
      assert ^idx_a = IndexInventory.select_index([idx_a, idx_b], constraints)
      assert ^idx_b = IndexInventory.select_index([idx_b, idx_a], constraints)
    end

    test "b over a" do
      idx_a = [id: :a, fields: [:timestamp]]
      idx_b = [id: :b, fields: [:user_id]]
      constraints = [%Equal{field: :user_id}]
      assert ^idx_b = IndexInventory.select_index([idx_a, idx_b], constraints)
      assert ^idx_b = IndexInventory.select_index([idx_b, idx_a], constraints)
    end

    test "exact matches with different between ordering" do
      idx_a = [id: :a, fields: [:date, :time, :user_id]]
      idx_b = [id: :b, fields: [:date, :user_id, :time]]
      constraints = [%Equal{field: :date}, %Equal{field: :user_id}, %Between{field: :time}]
      assert ^idx_b = IndexInventory.select_index([idx_a, idx_b], constraints)
      assert ^idx_b = IndexInventory.select_index([idx_b, idx_a], constraints)
    end

    test "one subset, the other insufficient" do
      idx_a = [id: :a, fields: [:date, :time, :user_id]]
      idx_b = [id: :b, fields: [:user_id, :time]]
      constraints = [%Equal{field: :date}]
      assert ^idx_a = IndexInventory.select_index([idx_a, idx_b], constraints)
      assert ^idx_a = IndexInventory.select_index([idx_b, idx_a], constraints)
    end

    test "inexact matches with different between ordering" do
      idx_a = [id: :a, fields: [:date, :time, :user_id, :extra]]
      idx_b = [id: :b, fields: [:date, :user_id, :time, :extra]]
      constraints = [%Equal{field: :date}, %Equal{field: :user_id}, %Between{field: :time}]
      assert ^idx_b = IndexInventory.select_index([idx_a, idx_b], constraints)
      assert ^idx_b = IndexInventory.select_index([idx_b, idx_a], constraints)
    end

    test "best partial match" do
      # Note: this will fail at the Default index, but we allow it to be selected at the inventory
      idx_a = [id: :a, fields: [:date, :time]]
      idx_b = [id: :b, fields: [:date, :user_id, :time]]
      constraints = [%Between{field: :time}]
      assert ^idx_a = IndexInventory.select_index([idx_a, idx_b], constraints)
      assert ^idx_a = IndexInventory.select_index([idx_b, idx_a], constraints)
    end
  end
end
