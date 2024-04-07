defmodule EctoFoundationDB.Layer.Pack do
  @moduledoc false
  # This internal module creates binaries to be used as FDB keys and values.

  # Primary writes are stored in
  #     {@adapter_prefix, source, @data_namespace, id}

  # Default indexes are stored in
  #     {@adapter_prefix, source, @index_namespace, index_name, length(index_values), [...index_values...], id}

  # Schema migrations are stored as primary writes and default indexes with
  #     {@migration_prefix, source, ...}

  @adapter_prefix <<0xFD>>
  @migration_prefix <<0xFE>>
  @data_namespace "d"
  @index_namespace "i"

  @doc """
  ## Examples

    iex> EctoFoundationDB.Layer.Pack.adapter_repo_range()
    {"\\x01\\xFD\\x00\\x00", "\\x01\\xFD\\x00\\xFF"}
  """
  def adapter_repo_range() do
    :erlfdb_tuple.range({@adapter_prefix})
  end

  @doc """
  ## Examples

  iex> EctoFoundationDB.Layer.Pack.adapter_source_range("my-source")
  {"\\x01\\xFD\\x00\\x01my-source\\x00\\x00", "\\x01\\xFD\\x00\\x01my-source\\x00\\xFF"}
  """
  def adapter_source_range(source) do
    :erlfdb_tuple.range({prefix(source), source})
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

    iex> EctoFoundationDB.Layer.Pack.primary_pack("my-source", "my-id")
    iex> |> :erlfdb_tuple.unpack()
    {"\\xFD", "my-source", "d", <<131, 109, 0, 0, 0, 5, 109, 121, 45, 105, 100>>}
  """
  def primary_pack(source, id) do
    namespaced_pack(source, @data_namespace, [encode_pk_for_key(id)])
  end

  @doc """
  ## Examples

    iex> EctoFoundationDB.Layer.Pack.primary_range("my-source")
    {"\\x01\\xFD\\0\\x01my-source\\0\\x01d\\0\\0", "\\x01\\xFD\\0\\x01my-source\\0\\x01d\\0\\xFF"}
  """
  def primary_range(source) do
    namespaced_range(source, @data_namespace, [])
  end

  def primary_mapper() do
    # prefix   source    namespace id        get_range
    {"{V[0]}", "{V[1]}", "{V[2]}", "{V[3]}", "{...}"}
  end

  @doc """
  We assume index_values are already encoded as binaries

  ## Examples

    iex> EctoFoundationDB.Layer.Pack.default_index_pack("my-source", "my-index", 1, ["my-val"], "my-id")
    iex> |> :erlfdb_tuple.unpack()
    {"\\xFD", "my-source", "i", "my-index", 1, "my-val", <<131, 109, 0, 0, 0, 5, 109, 121, 45, 105, 100>>}
  """
  def default_index_pack(source, index_name, idx_len, index_values, id) do
    vals =
      ["#{index_name}", idx_len] ++
        index_values ++ if(is_nil(id), do: [], else: [encode_pk_for_key(id)])

    namespaced_pack(source, @index_namespace, vals)
  end

  def default_index_range(source, index_name) do
    vals = ["#{index_name}"]
    namespaced_range(source, @index_namespace, vals)
  end

  @doc """
  We assume index_values are already encoded as binaries

  ## Examples

    iex> EctoFoundationDB.Layer.Pack.default_index_range("my-source", "my-index", 1, ["my-val"])
    {"\\x01\\xFD\\0\\x01my-source\\0\\x01i\\0\\x01my-index\\0\\x15\\x01\\x01my-val\\0\\0", "\\x01\\xFD\\0\\x01my-source\\0\\x01i\\0\\x01my-index\\0\\x15\\x01\\x01my-val\\0\\xFF"}
  """
  def default_index_range(source, index_name, idx_len, index_values) do
    vals = ["#{index_name}", idx_len] ++ index_values
    namespaced_range(source, @index_namespace, vals)
  end

  def namespaced_pack(source, namespace, vals) when is_list(vals) do
    ([prefix(source), source, namespace] ++ vals)
    |> :erlang.list_to_tuple()
    |> :erlfdb_tuple.pack()
  end

  def namespaced_range(source, namespace, vals) do
    ([prefix(source), source, namespace] ++ vals)
    |> :erlang.list_to_tuple()
    |> :erlfdb_tuple.range()
  end

  defp prefix("\xFF" <> _), do: @migration_prefix
  defp prefix(_), do: @adapter_prefix

  @doc """
  ## Examples

    iex> bin = EctoFoundationDB.Layer.Pack.to_fdb_value([id: "my-pk-id", x: "a-value", y: 42])
    iex> EctoFoundationDB.Layer.Pack.from_fdb_value(bin)
    [id: "my-pk-id", x: "a-value", y: 42]

  """
  def to_fdb_value(fields), do: :erlang.term_to_binary(fields)

  def from_fdb_value(bin), do: :erlang.binary_to_term(bin)

  def encode_pk_for_key(id), do: :erlang.term_to_binary(id)
end
