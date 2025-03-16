defmodule EctoFoundationDB.CLI.Internal do
  @moduledoc false

  alias EctoFoundationDB.Layer.DecodedKV
  alias EctoFoundationDB.Layer.Pack
  alias EctoFoundationDB.Layer.PrimaryKVCodec
  alias EctoFoundationDB.Schema

  alias Ecto.Adapters.FoundationDB

  def read_raw_primary_obj(tenant, schema, pk) do
    objs =
      FoundationDB.transactional(tenant, fn tx ->
        kv_codec = Pack.primary_codec(tenant, Schema.get_source(schema), pk)

        {start_key, end_key} = PrimaryKVCodec.range(kv_codec)

        tx
        |> :erlfdb.get_range(start_key, end_key, wait: true)
        |> PrimaryKVCodec.stream_decode(tenant)
        |> Enum.map(fn %DecodedKV{data_object: obj} -> obj end)
      end)

    case objs do
      [obj] ->
        obj

      [] ->
        nil
    end
  end
end
