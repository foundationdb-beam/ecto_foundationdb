defmodule EctoFoundationDB.Versionstamp do
  @moduledoc """
  Versionstamping is a feature that allows you to create a identifier for a record
  that is guaranteed to be unique across all records in the database.

  Please refer to the documentation for `Repo.async_insert_all!/3` for instructions
  on how to insert records with a Versionstamp primary key.

  ## Partition-by option

  When used as a primary key type, an optional `partition_by:` option can be provided to
  co-locate records with the same field value in the same keyspace:

      @primary_key {:id, {EctoFoundationDB.Versionstamp, partition_by: :user_id}, autogenerate: false}

  This enables efficient single-partition range scans:

      from(s in Session, where: s.id >= ^{"alice", checkpoint} and s.id < ^{"alice", Versionstamp.max()})
      |> Repo.all(prefix: tenant)
  """
  use Ecto.ParameterizedType

  alias EctoFoundationDB.Exception.Unsupported
  alias EctoFoundationDB.Future
  alias EctoFoundationDB.Layer.Tx

  # From :erlfdb_tuple
  @vs80 0x32
  @vs96 0x33
  @max_id 0xFFFFFFFFFFFFFFFF
  @max_batch 0xFFFF
  @max_user 0xFFFF

  @inc_id @max_id
  @inc_batch @max_batch

  def incomplete(user), do: {:versionstamp, @inc_id, @inc_batch, user}
  def max(), do: {:versionstamp, @max_id, @max_batch, @max_user}
  def min(), do: {:versionstamp, 0, 0, 0}

  def incomplete?({:versionstamp, @max_id, @max_batch, @max_user}), do: false
  def incomplete?({:versionstamp, @inc_id, @inc_batch, _}), do: true
  def incomplete?(_), do: false

  @doc """
  Returns true if the given value is a parameterized Versionstamp type term,
  as returned by `schema.__schema__(:type, field)`.
  """
  def type?({:parameterized, {__MODULE__, _}}), do: true
  def type?(_), do: false

  def get(tx) do
    Future.new(:erlfdb_future, :erlfdb.get_versionstamp(tx), &from_binary/1)
  end

  def to_integer(vs = {:versionstamp, @max_id, @max_batch, @max_user}), do: to_integer_(vs)

  def to_integer({:versionstamp, @inc_id, @inc_batch, _}) do
    raise Unsupported, """
    Versionstamps must be completed before they are useful, so we disallow converting an incomplete versionstamp to an integer.

    Versionstamp discovery can be done within the transaction that created it, and an incomplete versionstamp can be made complete with `resolve/2`.

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

  def to_integer(vs = {:versionstamp, _, _, _}), do: to_integer_(vs)

  defp to_integer_(vs = {:versionstamp, _, _, _}) do
    <<@vs96, bin::binary>> = :erlfdb_tuple.pack({vs})
    :binary.decode_unsigned(bin, :big)
  end

  def from_integer(int) when is_integer(int) do
    bin = :binary.encode_unsigned(int, :big)
    padding = :binary.copy(<<0>>, 12 - byte_size(bin))
    {vs} = :erlfdb_tuple.unpack(<<@vs96>> <> padding <> bin)
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
  def init(opts) do
    %{partition_by: Keyword.get(opts, :partition_by)}
  end

  @impl true
  def type(_params), do: :id

  @impl true
  def cast(nil, _params), do: {:ok, nil}
  def cast(id, _params) when is_integer(id), do: {:ok, from_integer(id)}
  def cast(vs = {:versionstamp, _, _, _}, _params), do: {:ok, vs}
  def cast(id_str, _params) when is_binary(id_str), do: Ecto.Type.cast(:id, id_str)

  def cast({partition_value, vs = {:versionstamp, _, _, _}}, %{partition_by: partition_by})
      when not is_nil(partition_by),
      do: {:ok, {partition_value, vs}}

  def cast(_, _params), do: :error

  @impl true
  def dump(nil, _dumper, _params), do: {:ok, nil}
  def dump(vs = {:versionstamp, _, _, _}, _dumper, _params), do: {:ok, vs}

  def dump({partition_value, vs = {:versionstamp, _, _, _}}, _dumper, %{
        partition_by: partition_by
      })
      when not is_nil(partition_by),
      do: {:ok, {partition_value, vs}}

  def dump(_, _, _), do: :error

  @impl true
  def load(vs = {:versionstamp, _, _, _}, _loader, _params), do: {:ok, vs}
  def load(_, _, _), do: :error
end
