defmodule EctoFoundationDBKVZipperTest do
  use ExUnit.Case

  alias EctoFoundationDB.Layer.KVZipper
  alias EctoFoundationDB.Test.Util

  test "zipper" do
    tenant = %EctoFoundationDB.Tenant{backend: EctoFoundationDB.Tenant.ManagedTenant}

    a_zipper = KVZipper.new({"a"})
    a_data = [id: Util.get_random_bytes(82)]
    a_value = :erlang.term_to_binary(a_data)
    assert 100 = byte_size(a_value)
    assert {true, a_kvs} = KVZipper.unzip(a_zipper, a_value, max_single_value_size: 10)

    # 100 bytes split into 10 kvs each of size 10 bytes. And 1 extra kv for the "primary write" which holds the metadata
    assert 11 = length(a_kvs)

    assert [{%EctoFoundationDB.Layer.KVZipper{tuple: {"a"}}, ^a_data}] =
             KVZipper.stream_zip(a_kvs, tenant) |> Enum.to_list()

    b_zipper = KVZipper.new({"b"})
    b_data = [id: "xx"]
    b_value = :erlang.term_to_binary(b_data)
    assert 20 = byte_size(b_value)
    assert {false, b_kvs} = KVZipper.unzip(b_zipper, b_value, max_single_value_size: 20)
    assert 1 = length(b_kvs)

    assert [
             {%EctoFoundationDB.Layer.KVZipper{tuple: {"a"}}, ^a_data},
             {%EctoFoundationDB.Layer.KVZipper{tuple: {"b"}}, ^b_data}
           ] =
             KVZipper.stream_zip(a_kvs ++ b_kvs, tenant) |> Enum.to_list()
  end
end
