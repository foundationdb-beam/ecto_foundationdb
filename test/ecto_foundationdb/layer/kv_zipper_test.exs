defmodule EctoFoundationDBPrimaryKVCodecTest do
  use ExUnit.Case

  alias EctoFoundationDB.Layer.DecodedKV
  alias EctoFoundationDB.Layer.PrimaryKVCodec
  alias EctoFoundationDB.Test.Util

  test "kv_codec" do
    tenant = %EctoFoundationDB.Tenant{backend: EctoFoundationDB.Tenant.ManagedTenant}

    a_codec = PrimaryKVCodec.new({"a"})
    a_data = [id: Util.get_random_bytes(82)]
    a_value = :erlang.term_to_binary(a_data)
    assert 100 = byte_size(a_value)
    assert {true, a_kvs} = PrimaryKVCodec.encode(a_codec, a_value, max_single_value_size: 10)

    # 100 bytes split into 10 kvs each of size 10 bytes. And 1 extra kv for the "primary write" which holds the metadata
    assert 11 = length(a_kvs)

    assert [
             %DecodedKV{
               codec: %EctoFoundationDB.Layer.PrimaryKVCodec{tuple: {"a"}},
               data_object: ^a_data,
               multikey?: true
             }
           ] =
             PrimaryKVCodec.stream_decode(a_kvs, tenant) |> Enum.to_list()

    b_codec = PrimaryKVCodec.new({"b"})
    b_data = [id: "xx"]
    b_value = :erlang.term_to_binary(b_data)
    assert 20 = byte_size(b_value)
    assert {false, b_kvs} = PrimaryKVCodec.encode(b_codec, b_value, max_single_value_size: 20)
    assert 1 = length(b_kvs)

    assert [
             %DecodedKV{
               codec: %EctoFoundationDB.Layer.PrimaryKVCodec{tuple: {"a"}},
               data_object: ^a_data,
               multikey?: true
             },
             %DecodedKV{
               codec: %EctoFoundationDB.Layer.PrimaryKVCodec{tuple: {"b"}},
               data_object: ^b_data,
               multikey?: false
             }
           ] =
             PrimaryKVCodec.stream_decode(a_kvs ++ b_kvs, tenant) |> Enum.to_list()
  end
end
