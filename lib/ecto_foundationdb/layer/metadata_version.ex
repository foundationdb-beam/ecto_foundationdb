defmodule EctoFoundationDB.Layer.MetadataVersion do
  @moduledoc false
  @fdb_metadata_version_key "\xff/metadataVersion"
  @fdb_metadata_version_required_value String.duplicate("\0", 14)

  alias EctoFoundationDB.Future
  alias EctoFoundationDB.Layer.Pack

  import Kernel, except: [match?: 2]

  defstruct [:global, :app]

  def new(global, app \\ nil), do: %__MODULE__{global: global, app: app}

  def match_global?(%__MODULE__{global: global}, %__MODULE__{global: global}),
    do: not is_nil(global)

  def match_global?(_a, _b), do: false

  def match_local?(nil, _), do: false
  def match_local?(_, nil), do: false

  def match_local?(mdv_a = %__MODULE__{}, mdv_b = %__MODULE__{}),
    do: match?(%{mdv_a | global: <<>>}, %{mdv_b | global: <<>>})

  def match?(mdv, mdv), do: not is_nil(mdv)
  def match?(_, _), do: false

  def tx_set_global(tx) do
    :erlfdb.set_versionstamped_value(
      tx,
      @fdb_metadata_version_key,
      @fdb_metadata_version_required_value
    )
  end

  def tx_set_app(tenant, tx, idx, val) do
    :erlfdb.max(tx, app_version_key(tenant, idx), val)
  end

  def tx_get_new(tx, future) do
    Future.set(future, tx, :erlfdb.get(tx, @fdb_metadata_version_key), &new/1)
  end

  def tx_with_app(mdv = %__MODULE__{app: nil}, tenant, tx, idx, future) do
    Future.set(
      future,
      tx,
      :erlfdb.get(tx, app_version_key(tenant, idx)),
      &with_decoded_app(mdv, &1)
    )
  end

  def tx_get_full(tenant, tx, idx, future) do
    fut1 = Future.set(future, tx, :erlfdb.get(tx, @fdb_metadata_version_key))
    fut2 = Future.set(future, tx, :erlfdb.get(tx, app_version_key(tenant, idx)), &decode_app/1)

    [global, app] =
      [fut1, fut2]
      |> Future.await_all()
      |> Enum.map(&Future.result/1)

    new(global, app)
  end

  defp with_decoded_app(mdv = %__MODULE__{}, app_result) do
    %{mdv | app: decode_app(app_result)}
  end

  defp decode_app(:not_found), do: -1
  defp decode_app(x), do: :binary.decode_unsigned(x, :little)

  def app_version_key(tenant, idx) do
    index_name = idx[:id]
    source = idx[:source]
    Pack.namespaced_pack(tenant, source, "max", ["#{index_name}"])
  end
end
