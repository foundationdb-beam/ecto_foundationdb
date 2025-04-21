defmodule EctoFoundationDB.Versionstamp do
  @moduledoc """
  Versionstamping is a feature that allows you to create a unique identifier for a record
  that is guaranteed to be unique across all records in the database.

  Please refer to the documentation for `Repo.async_insert_all!/3`.
  """
  use Ecto.Type

  alias EctoFoundationDB.Exception.Unsupported
  alias EctoFoundationDB.Future
  alias EctoFoundationDB.Layer.Tx

  # From :erlfdb_tuple
  @vs80 0x32
  @vs96 0x33
  @inc_id 0xFFFFFFFFFFFFFFFF
  @inc_batch 0xFFFF

  def incomplete(user) do
    {:versionstamp, @inc_id, @inc_batch, user}
  end

  def incomplete?({:versionstamp, @inc_id, @inc_batch, _}), do: true
  def incomplete?(_), do: false

  def get(tx) do
    Future.new_deferred(:erlfdb.get_versionstamp(tx), &from_binary/1)
  end

  def to_integer({:versionstamp, @inc_id, @inc_batch, _}) do
    raise Unsupported, """
    Versionstamps must be completed before they are useful, so we disallow converting an incomplete versionstamp to an integer.

    Verstionstamp discovery can be done within the transaction that created it, and an incomplete versionstamp can be made complete with `resolve/2`.

        alias EctoFoundationDB.Future
        alias EctoFoundationDB.Versionstamp

        {event, vs_future} = MyRepo.transactional(tenant, fn tx ->
          {:ok, event} = MyRepo.insert(%Event{id: Versionstamp.next(tx)})
          vs_future = Versionstamp.get(tx)
          {event, vs_future}
        end)

        vs = MyRepo.await(vs_future)
        event = %{event | id: Versionstamp.resolve(event.id, vs)}
    """
  end

  def to_integer(vs = {:versionstamp, _, _, _}) do
    <<@vs96, bin::binary>> = :erlfdb_tuple.pack({vs})
    :binary.decode_unsigned(bin, :big)
  end

  def from_integer(int) when is_integer(int) do
    bin = :binary.encode_unsigned(int, :big)
    {vs} = :erlfdb_tuple.unpack(<<@vs96>> <> bin)
    vs
  end

  def from_binary(bin) when byte_size(bin) == 10 do
    {vs80} = :erlfdb_tuple.unpack(<<@vs80>> <> bin)
    vs80
  end

  def next() do
    if Tx.in_tx?() do
      raise Unsupported, """
      When calling from inside a transaction, you must use `EctoFoundationDB.Versionstamp.next/1`.
      """
    end

    incomplete(0)
  end

  def next(tx) do
    incomplete(:erlfdb.get_next_tx_id(tx))
  end

  def resolve({:versionstamp, @inc_id, @inc_batch, user}, {:versionstamp, id, batch}) do
    {:versionstamp, id, batch, user}
  end

  @impl true
  def type(), do: :id

  @impl true
  def cast(id) when is_integer(id), do: {:ok, from_integer(id)}
  def cast(vs = {:versionstamp, _, _, _}), do: {:ok, vs}
  def cast(id_str) when is_binary(id_str), do: Ecto.Type.cast(:id, id_str)
  def cast(_), do: :error

  @impl true
  def dump(vs = {:versionstamp, _, _, _}), do: {:ok, vs}

  @impl true
  def load(vs = {:versionstamp, _, _, _}), do: {:ok, vs}
end
