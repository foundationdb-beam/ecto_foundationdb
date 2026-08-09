defmodule EctoFoundationDBIndexerDefaultTest do
  use ExUnit.Case, async: true

  alias EctoFoundationDB.Exception.Unsupported
  alias EctoFoundationDB.Indexer.Default
  alias EctoFoundationDB.Layer.Pack
  alias EctoFoundationDB.QueryPlan
  alias EctoFoundationDB.Schemas.District
  alias EctoFoundationDB.Schemas.User
  alias EctoFoundationDB.Tenant

  doctest EctoFoundationDB.Indexer.Default

  defp tenant(), do: %Tenant{backend: Tenant.ManagedTenant}

  defp idx(id, source, fields, options \\ []) do
    [id: id, type: :index, indexer: Default, source: source, fields: fields, options: options]
  end

  defp plan(source, schema, constraints) do
    %QueryPlan{
      tenant: tenant(),
      source: source,
      schema: schema,
      context: [],
      constraints: constraints,
      updates: [],
      layer_data: %EctoFoundationDB.Layer.Query{},
      ordering: [],
      limit: nil
    }
  end

  describe "index fields overlapping the primary key" do
    # The primary key is already appended to every index key. Naming it again
    # would strip it from the index values and reintroduce it after every other
    # index field, which is both the wrong sort order and a write/read
    # disagreement on idx_len. We refuse the shape so no database persists keys
    # under a layout that is still undecided.
    test "a single-field key named in the index is rejected" do
      idx = idx("users_id_index", "users", [:id])
      plan = plan("users", User, [%QueryPlan.Equal{field: :id, pk?: true, param: "u-1"}])

      assert_raise Unsupported, ~r/include a primary key field/, fn ->
        Default.range(idx, plan, [])
      end
    end

    test "a composite key field named in the index is rejected" do
      idx = idx("districts_d_w_id_d_name_index", "districts", [:d_w_id, :d_name])
      plan = plan("districts", District, [%QueryPlan.None{pk?: true, fields: []}])

      assert_raise Unsupported, ~r/include a primary key field/, fn ->
        Default.range(idx, plan, [])
      end
    end

    test "the message names the offending fields" do
      idx = idx("districts_bad_index", "districts", [:d_name, :d_id])
      plan = plan("districts", District, [%QueryPlan.None{pk?: true, fields: []}])

      err =
        assert_raise Unsupported, fn ->
          Default.range(idx, plan, [])
        end

      assert err.message =~ "offending: [:d_id]"
      assert err.message =~ "primary key: [:d_w_id, :d_id]"
    end

    test "an index over non-key fields is unaffected" do
      idx = idx("users_name_index", "users", [:name], mapped?: false)
      plan = plan("users", User, [%QueryPlan.None{pk?: true, fields: []}])

      assert {_start_key, _end_key} = Default.range(idx, plan, [])
    end

    test "a schemaless plan skips the check" do
      idx = idx("users_name_index", "users", [:name], mapped?: false)
      plan = plan("users", nil, [%QueryPlan.None{pk?: true, fields: []}])

      assert {_start_key, _end_key} = Default.range(idx, plan, [])
    end
  end

  describe "idx_len" do
    test "the read range brackets the key the write path produces" do
      # idx_len is persisted in the key, so the count the write path splices in
      # and the count the read path puts in its range prefix have to agree.
      # Deriving either from the schema rather than from the values is what
      # breaks this.
      tenant = tenant()

      key = Pack.default_index_pack(tenant, "users", "users_name_index", 1, ["Alice"], "u-1")

      idx = idx("users_name_index", "users", [:name], mapped?: false)
      plan = plan("users", User, [%QueryPlan.Equal{field: :name, pk?: false, param: "Alice"}])

      {start_key, end_key} = Default.range(idx, plan, [])

      assert start_key <= key and key <= end_key
    end
  end
end
