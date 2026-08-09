defmodule Ecto.Integration.CompositePKEdgeTest do
  @moduledoc """
  Edge cases around composite primary keys, and the backwards compatibility
  guarantee that a single-field key still encodes exactly as it did.
  """
  use Ecto.Integration.Case, async: true

  import Ecto.Query

  alias Ecto.Integration.TestRepo
  alias EctoFoundationDB.Layer.Fields
  alias EctoFoundationDB.Layer.Pack
  alias EctoFoundationDB.Layer.PrimaryKVCodec
  alias EctoFoundationDB.Schemas.District
  alias EctoFoundationDB.Schemas.User

  @moduletag :integration

  defp put(tenant, w, d, name) do
    TestRepo.insert!(
      %District{d_w_id: w, d_id: d, d_name: name, d_next_o_id: 3001},
      prefix: tenant
    )
  end

  describe "backwards compatibility" do
    test "a single-field key encodes to the same bytes as before" do
      tenant = %EctoFoundationDB.Tenant{backend: EctoFoundationDB.Tenant.ManagedTenant}

      %{packed: packed} =
        Pack.primary_codec(tenant, "my-source", "my-id") |> PrimaryKVCodec.with_packed_key()

      assert {"\xFD", "my-source", "d", "my-id"} =
               EctoFoundationDB.Tenant.unpack(tenant, packed)
    end

    test "a single-field schema still round-trips", context do
      tenant = context[:tenant]
      user = TestRepo.insert!(%User{name: "Alice"}, prefix: tenant)

      assert %User{name: "Alice"} = TestRepo.get(User, user.id, prefix: tenant)
    end

    test "get_pk_field!/1 still returns the single field" do
      assert :id = Fields.get_pk_field!(User)
    end

    test "get_pk_field!/1 raises a described error for a composite key" do
      assert_raise ArgumentError, ~r/composite primary key/, fn ->
        Fields.get_pk_field!(District)
      end
    end
  end

  describe "where clause shape" do
    test "field order in the where clause does not matter", context do
      tenant = context[:tenant]
      put(tenant, 1, 2, "target")
      put(tenant, 2, 1, "other")

      assert [%District{d_name: "target"}] =
               from(d in District, where: d.d_id == ^2 and d.d_w_id == ^1)
               |> TestRepo.all(prefix: tenant)
    end

    test "constraining only a trailing key field is rejected, not silently wrong", context do
      tenant = context[:tenant]
      put(tenant, 1, 2, "a")

      assert_raise EctoFoundationDB.Exception.Unsupported, fn ->
        from(d in District, where: d.d_id == ^2)
        |> TestRepo.all(prefix: tenant)
      end
    end

    test "a full scan still works", context do
      tenant = context[:tenant]
      put(tenant, 1, 1, "a")
      put(tenant, 2, 2, "b")

      assert 2 = District |> TestRepo.all(prefix: tenant) |> length()
    end
  end

  describe "bulk operations" do
    test "update_all over a key prefix", context do
      tenant = context[:tenant]
      for d <- 1..3, do: put(tenant, 1, d, "w1")
      put(tenant, 2, 1, "w2")

      assert {3, _} =
               from(d in District, where: d.d_w_id == ^1)
               |> TestRepo.update_all([set: [d_next_o_id: 9999]], prefix: tenant)

      assert [9999, 9999, 9999] =
               from(d in District, where: d.d_w_id == ^1)
               |> TestRepo.all(prefix: tenant)
               |> Enum.map(& &1.d_next_o_id)

      assert [3001] =
               from(d in District, where: d.d_w_id == ^2)
               |> TestRepo.all(prefix: tenant)
               |> Enum.map(& &1.d_next_o_id)
    end

    test "update_all keeps every key field intact", context do
      tenant = context[:tenant]
      put(tenant, 1, 2, "before")

      from(d in District, where: d.d_w_id == ^1 and d.d_id == ^2)
      |> TestRepo.update_all([set: [d_name: "after"]], prefix: tenant)

      assert [%District{d_w_id: 1, d_id: 2, d_name: "after"}] =
               from(d in District, where: d.d_w_id == ^1)
               |> TestRepo.all(prefix: tenant)
    end

    test "delete_all over a key prefix", context do
      tenant = context[:tenant]
      for d <- 1..3, do: put(tenant, 1, d, "w1")
      put(tenant, 2, 1, "w2")

      assert {3, _} =
               from(d in District, where: d.d_w_id == ^1)
               |> TestRepo.delete_all(prefix: tenant)

      assert [] = from(d in District, where: d.d_w_id == ^1) |> TestRepo.all(prefix: tenant)

      assert 1 =
               from(d in District, where: d.d_w_id == ^2)
               |> TestRepo.all(prefix: tenant)
               |> length()
    end
  end

  describe "split records" do
    # PrimaryKVCodec splits a value over max_single_value_size (100_000
    # bytes) across several keys, appending to the key tuple. A composite
    # prefix scan must reassemble those, not surface fragments.
    test "a record split across keys survives a prefix scan", context do
      tenant = context[:tenant]
      big = String.duplicate("x", 250_000)

      TestRepo.insert!(
        %District{d_w_id: 7, d_id: 1, d_name: big, d_next_o_id: 1},
        prefix: tenant
      )

      put(tenant, 7, 2, "small")

      rows =
        from(d in District, where: d.d_w_id == ^7)
        |> TestRepo.all(prefix: tenant)
        |> Enum.sort_by(& &1.d_id)

      assert [%District{d_id: 1, d_name: ^big}, %District{d_id: 2, d_name: "small"}] = rows
    end

    test "a split record is addressable by its full composite key", context do
      tenant = context[:tenant]
      big = String.duplicate("y", 250_000)

      TestRepo.insert!(
        %District{d_w_id: 8, d_id: 3, d_name: big, d_next_o_id: 1},
        prefix: tenant
      )

      assert [%District{d_name: ^big}] =
               from(d in District, where: d.d_w_id == ^8 and d.d_id == ^3)
               |> TestRepo.all(prefix: tenant)
    end
  end

  describe "key encoding" do
    test "negative and large values keep numeric order", context do
      tenant = context[:tenant]
      for d <- [-5, 0, 3, 1_000_000], do: put(tenant, 1, d, "d#{d}")

      assert [-5, 0, 3, 1_000_000] =
               from(d in District, where: d.d_w_id == ^1)
               |> TestRepo.all(prefix: tenant)
               |> Enum.map(& &1.d_id)
    end

    test "one warehouse's prefix scan never leaks another's", context do
      tenant = context[:tenant]
      put(tenant, 1, 1, "w1")
      put(tenant, 11, 1, "w11")
      put(tenant, 111, 1, "w111")

      assert [%District{d_name: "w1"}] =
               from(d in District, where: d.d_w_id == ^1)
               |> TestRepo.all(prefix: tenant)

      assert [%District{d_name: "w11"}] =
               from(d in District, where: d.d_w_id == ^11)
               |> TestRepo.all(prefix: tenant)
    end
  end
end
