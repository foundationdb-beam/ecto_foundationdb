defmodule Ecto.Integration.CompositePKTest do
  @moduledoc """
  Composite natural primary keys, in the shape TPC-C requires.

  TPC-C identifies its tables by compound natural attributes, for example
  `DISTRICT` by `(D_W_ID, D_ID)`. Before this, the adapter accepted one
  primary key field only, so those schemas raised `MatchError`. The
  workaround was a synthetic key plus an index, which changes the physical
  layout and therefore the contention the benchmark produces.
  """
  use Ecto.Integration.Case, async: true

  import Ecto.Query

  alias Ecto.Integration.TestRepo
  alias EctoFoundationDB.Schemas.District

  @moduletag :integration

  defp put(tenant, w, d, name) do
    TestRepo.insert!(
      %District{d_w_id: w, d_id: d, d_name: name, d_next_o_id: 3001},
      prefix: tenant
    )
  end

  describe "writes" do
    test "inserts and reads back every key field", context do
      tenant = context[:tenant]
      put(tenant, 1, 2, "d-1-2")

      assert [%District{d_w_id: 1, d_id: 2, d_name: "d-1-2", d_next_o_id: 3001}] =
               from(d in District, where: d.d_w_id == ^1 and d.d_id == ^2)
               |> TestRepo.all(prefix: tenant)
    end

    test "distinguishes records differing in only one key field", context do
      tenant = context[:tenant]
      put(tenant, 1, 1, "a")
      put(tenant, 1, 2, "b")
      put(tenant, 2, 1, "c")

      assert [%District{d_name: "b"}] =
               from(d in District, where: d.d_w_id == ^1 and d.d_id == ^2)
               |> TestRepo.all(prefix: tenant)

      assert [%District{d_name: "c"}] =
               from(d in District, where: d.d_w_id == ^2 and d.d_id == ^1)
               |> TestRepo.all(prefix: tenant)
    end

    test "rejects a nil key field", context do
      tenant = context[:tenant]

      assert_raise EctoFoundationDB.Exception.Unsupported, fn ->
        TestRepo.insert!(%District{d_w_id: 1, d_id: nil, d_name: "x"}, prefix: tenant)
      end
    end
  end

  describe "prefix range scan" do
    test "a leading key field alone resolves without an index", context do
      tenant = context[:tenant]
      for d <- 1..10, do: put(tenant, 1, d, "w1-d#{d}")
      for d <- 1..3, do: put(tenant, 2, d, "w2-d#{d}")

      assert 10 =
               from(d in District, where: d.d_w_id == ^1)
               |> TestRepo.all(prefix: tenant)
               |> length()

      assert 3 =
               from(d in District, where: d.d_w_id == ^2)
               |> TestRepo.all(prefix: tenant)
               |> length()
    end

    test "the scan returns key order, not insertion order", context do
      tenant = context[:tenant]
      for d <- [7, 2, 9, 1], do: put(tenant, 1, d, "d#{d}")

      assert [1, 2, 7, 9] =
               from(d in District, where: d.d_w_id == ^1)
               |> TestRepo.all(prefix: tenant)
               |> Enum.map(& &1.d_id)
    end
  end

  describe "update and delete" do
    test "updates by the full composite key", context do
      tenant = context[:tenant]
      district = put(tenant, 1, 2, "before")

      district
      |> Ecto.Changeset.change(d_next_o_id: 3002)
      |> TestRepo.update!(prefix: tenant)

      assert [%District{d_next_o_id: 3002}] =
               from(d in District, where: d.d_w_id == ^1 and d.d_id == ^2)
               |> TestRepo.all(prefix: tenant)
    end

    test "deletes only the addressed record", context do
      tenant = context[:tenant]
      a = put(tenant, 1, 1, "a")
      _b = put(tenant, 1, 2, "b")

      TestRepo.delete!(a, prefix: tenant)

      assert [%District{d_id: 2}] =
               from(d in District, where: d.d_w_id == ^1)
               |> TestRepo.all(prefix: tenant)
    end
  end
end
