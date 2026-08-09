defmodule EctoFoundationDBLayerPackTest do
  use ExUnit.Case, async: true

  alias EctoFoundationDB.Exception.Unsupported
  alias EctoFoundationDB.Layer.Fields.CompositePK
  alias EctoFoundationDB.Layer.Pack
  alias EctoFoundationDB.Layer.PrimaryKVCodec
  alias EctoFoundationDB.Tenant
  alias EctoFoundationDB.Versionstamp

  doctest EctoFoundationDB.Layer.Pack

  defp tenant(), do: %Tenant{backend: Tenant.ManagedTenant}

  describe "composite primary key encoding" do
    test "an incomplete versionstamp is refused, not packed as its placeholder" do
      # `:erlfdb_tuple.pack/1` would happily encode the all-0xFF placeholder
      # literally, putting every such record on the same key with no way to
      # recover the real stamp.
      pk = %CompositePK{values: [1, Versionstamp.next()]}

      assert_raise Unsupported, ~r/Versionstamp within a composite primary key/, fn ->
        Pack.primary_codec(tenant(), "my-source", pk)
      end
    end

    test "an incomplete versionstamp is refused in any position" do
      pk = %CompositePK{values: [Versionstamp.next(), 1]}

      assert_raise Unsupported, ~r/Versionstamp within a composite primary key/, fn ->
        Pack.primary_codec(tenant(), "my-source", pk)
      end
    end

    test "a complete versionstamp is still packable" do
      pk = %CompositePK{values: [1, Versionstamp.min()]}

      %{packed: packed} =
        Pack.primary_codec(tenant(), "my-source", pk) |> PrimaryKVCodec.with_packed_key()

      assert {"\xFD", "my-source", "d", 1, {:versionstamp, 0, 0, 0}} =
               Tenant.unpack(tenant(), packed)
    end

    test "key fields are spliced as separate elements, so a prefix range covers them" do
      tenant = tenant()

      %{packed: packed} =
        Pack.primary_codec(tenant, "districts", %CompositePK{values: [1, 2]})
        |> PrimaryKVCodec.with_packed_key()

      assert {"\xFD", "districts", "d", 1, 2} = Tenant.unpack(tenant, packed)

      {start_key, end_key} = Pack.primary_prefix_range(tenant, "districts", [1])
      assert start_key <= packed and packed <= end_key
    end
  end
end
