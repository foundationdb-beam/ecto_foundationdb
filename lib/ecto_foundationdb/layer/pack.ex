defmodule EctoFoundationDB.Layer.Pack do
  @moduledoc false
  # This internal module creates binaries to be used as FDB keys and values.

  # Primary writes are stored in
  #     {@adapter_prefix, source, @data_namespace, id}

  # Default indexes and other metadata are stored in
  #     {@adapter_prefix, source, @index_namespace, index_name, length(index_values), [...index_values...], id}

  # Schema migrations are stored as primary writes and default indexes with
  #     {@migration_prefix, source, ...}

  alias EctoFoundationDB.Exception.Unsupported
  alias EctoFoundationDB.Tenant
  alias EctoFoundationDB.Versionstamp

  @adapter_prefix <<0xFD>>
  @migration_prefix <<0xFE>>
  @data_namespace "d"
  @index_namespace "i"

  @doc """
  ## Examples

    iex> tenant = %EctoFoundationDB.Tenant{backend: EctoFoundationDB.Tenant.ManagedTenant}
    iex> EctoFoundationDB.Layer.Pack.adapter_repo_range(tenant)
    {"\\x01\\xFD\\x00\\x00", "\\x01\\xFD\\x00\\xFF"}
  """
  def adapter_repo_range(tenant) do
    Tenant.range(tenant, {@adapter_prefix})
  end

  @doc """
  ## Examples

  iex> tenant = %EctoFoundationDB.Tenant{backend: EctoFoundationDB.Tenant.ManagedTenant}
  iex> EctoFoundationDB.Layer.Pack.adapter_source_range(tenant, "my-source")
  {"\\x01\\xFD\\x00\\x01my-source\\x00\\x00", "\\x01\\xFD\\x00\\x01my-source\\x00\\xFF"}
  """
  def adapter_source_range(tenant, source) do
    Tenant.range(tenant, {prefix(source), source})
  end

  @doc """
  This function computes a key for use in FDB to store the data object via the primary key.

  Ideally we could leave strings in the key directly so they are more easily human-readable.
  However, if two distinct primary keys encode to the same datakey from the output of this function,
  then the objects can be put into an inconsistent state.

  For this reason, the only safe approach is to encode all terms with the same mechanism.
  We choose `:erlang.term_to_binary()` for this encoding.

  If you need to inspect the raw keys stored in FDB, we suggest you use the following

  ```elixir
  inspect(key, binaries: :as_strings)
  ```

  ## Examples

    iex> alias EctoFoundationDB.Layer.PrimaryKVCodec
    iex> tenant = %EctoFoundationDB.Tenant{backend: EctoFoundationDB.Tenant.ManagedTenant}
    iex> %{packed: packed} = EctoFoundationDB.Layer.Pack.primary_codec(tenant, "my-source", "my-id") |> PrimaryKVCodec.with_packed_key()
    iex> EctoFoundationDB.Tenant.unpack(tenant, packed)
    {"\\xFD", "my-source", "d", "my-id"}
  """
  def primary_codec(tenant, source, id) do
    namespaced_tuple(source, @data_namespace, [encode_pk_for_key(id)])
    |> then(&Tenant.primary_codec(tenant, &1, Versionstamp.incomplete?(id)))
  end

  def primary_write_key_to_codec(tenant, key) when is_binary(key) do
    tenant
    |> Tenant.unpack(key)
    |> then(&primary_write_key_to_codec(tenant, &1))
  end

  def primary_write_key_to_codec(tenant, tuple) do
    Tenant.primary_codec(tenant, tuple, false)
  end

  def get_vs_from_key_tuple(key_tuple) do
    case Kernel.elem(key_tuple, tuple_size(key_tuple) - 1) do
      vs = {:versionstamp, _, _, _} ->
        vs

      _ ->
        raise Unsupported, """
        Versionstamp must be the last element of the key tuple. Instead we found

        #{inspect(key_tuple)}
        """
    end
  end

  @doc """
  ## Examples

    iex> tenant = %EctoFoundationDB.Tenant{backend: EctoFoundationDB.Tenant.ManagedTenant}
    iex> EctoFoundationDB.Layer.Pack.primary_range(tenant, "my-source")
    {"\\x01\\xFD\\0\\x01my-source\\0\\x01d\\0\\0", "\\x01\\xFD\\0\\x01my-source\\0\\x01d\\0\\xFF"}
  """
  def primary_range(tenant, source) do
    namespaced_range(tenant, source, @data_namespace, [])
  end

  @doc """
  We assume index_values are already encoded as binaries

  ## Examples

    iex> tenant = %EctoFoundationDB.Tenant{backend: EctoFoundationDB.Tenant.ManagedTenant}
    iex> EctoFoundationDB.Layer.Pack.default_index_pack(tenant, "my-source", "my-index", 1, ["my-val"], "my-id")
    iex> |> then(&EctoFoundationDB.Tenant.unpack(tenant, &1))
    {"\\xFD", "my-source", "i", "my-index", 1, "my-val", "my-id"}
  """
  def default_index_pack(tenant, source, index_name, idx_len, index_values, id) do
    vals = default_index_pack_vals(index_name, idx_len, index_values, id)
    namespaced_pack(tenant, source, @index_namespace, vals)
  end

  def default_index_pack_vs(tenant, source, index_name, idx_len, index_values, id) do
    vals = default_index_pack_vals(index_name, idx_len, index_values, id)
    namespaced_pack_vs(tenant, source, @index_namespace, vals)
  end

  defp default_index_pack_vals(index_name, idx_len, index_values, id) do
    ["#{index_name}", idx_len] ++
      index_values ++ if(is_nil(id), do: [], else: [encode_pk_for_key(id)])
  end

  def default_index_range(tenant, source, index_name) do
    vals = ["#{index_name}"]
    namespaced_range(tenant, source, @index_namespace, vals)
  end

  @doc """
  We assume index_values are already encoded as binaries

  ## Examples

    iex> tenant = %EctoFoundationDB.Tenant{backend: EctoFoundationDB.Tenant.ManagedTenant}
    iex> EctoFoundationDB.Layer.Pack.default_index_range(tenant, "my-source", "my-index", 1, ["my-val"])
    {"\\x01\\xFD\\0\\x01my-source\\0\\x01i\\0\\x01my-index\\0\\x15\\x01\\x01my-val\\0\\0", "\\x01\\xFD\\0\\x01my-source\\0\\x01i\\0\\x01my-index\\0\\x15\\x01\\x01my-val\\0\\xFF"}
  """
  def default_index_range(tenant, source, index_name, idx_len, index_values) do
    vals = ["#{index_name}", idx_len] ++ index_values
    namespaced_range(tenant, source, @index_namespace, vals)
  end

  def namespaced_pack(tenant, source, namespace, vals) do
    namespaced_tuple(source, namespace, vals)
    |> then(&Tenant.pack(tenant, &1))
  end

  def namespaced_pack_vs(tenant, source, namespace, vals) do
    namespaced_tuple(source, namespace, vals)
    |> then(&Tenant.pack_vs(tenant, &1))
  end

  defp namespaced_tuple(source, namespace, vals) when is_list(vals) do
    ([prefix(source), source, namespace] ++ vals)
    |> :erlang.list_to_tuple()
  end

  def namespaced_range(tenant, source, namespace, vals) do
    ([prefix(source), source, namespace] ++ vals)
    |> :erlang.list_to_tuple()
    |> then(&Tenant.range(tenant, &1))
  end

  defp prefix("schema_migrations"), do: @migration_prefix
  defp prefix("indexes"), do: @migration_prefix
  defp prefix(_), do: @adapter_prefix

  @doc """
  ## Examples

    iex> bin = EctoFoundationDB.Layer.Pack.to_fdb_value([id: "my-pk-id", x: "a-value", y: 42])
    iex> EctoFoundationDB.Layer.Pack.from_fdb_value(bin)
    [id: "my-pk-id", x: "a-value", y: 42]

  """
  def to_fdb_value(fields), do: :erlang.term_to_binary(fields)

  def from_fdb_value(bin), do: :erlang.binary_to_term(bin)

  def encode_pk_for_key(id = {:versionstamp, _, _, _}), do: id
  def encode_pk_for_key(id) when is_binary(id), do: id
  def encode_pk_for_key(id) when is_atom(id), do: {:utf8, "#{id}"}
  def encode_pk_for_key(id) when is_number(id), do: id
  def encode_pk_for_key(id), do: :erlang.term_to_binary(id)
end
