defmodule Ecto.Adapters.FoundationDB.Layer.Pack do
  @moduledoc """
  This internal module creates binaries to be used as FDB keys and values.
  """
  alias Ecto.Adapters.FoundationDB.Options

  @adapter_prefix <<0xFE>>
  @data_namespace "d"

  @doc """
  ## Examples

    iex> Ecto.Adapters.FoundationDB.Layer.Pack.add_delimiter("my-key", [])
    "my-key/"

    iex> Ecto.Adapters.FoundationDB.Layer.Pack.add_delimiter("my-key", [key_delimiter: <<0>>])
    "my-key\\0"

  """
  def add_delimiter(key, adapter_opts) do
    key <> Options.get(adapter_opts, :key_delimiter)
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

    iex> Ecto.Adapters.FoundationDB.Layer.Pack.to_fdb_datakey([], "table", "my-pk-id")
    "\\xFEtable/d/\\x83m\\0\\0\\0\\bmy-pk-id"

    iex> Ecto.Adapters.FoundationDB.Layer.Pack.to_fdb_datakey([], "table", :"my-pk-id")
    "\\xFEtable/d/\\x83w\\bmy-pk-id"

    iex> Ecto.Adapters.FoundationDB.Layer.Pack.to_fdb_datakey([], "table", {:tuple, :term})
    "\\xFEtable/d/\\x83h\\x02w\\x05tuplew\\x04term"

  """
  def to_fdb_datakey(adapter_opts, source, x) do
    to_raw_fdb_key(adapter_opts, [source, @data_namespace, encode_pk_for_key(x)])
  end

  @doc """
  ## Examples

    iex> Ecto.Adapters.FoundationDB.Layer.Pack.to_fdb_datakey_startswith([], "table")
    "\\xFEtable/d/"

  """
  def to_fdb_datakey_startswith(adapter_opts, source) do
    to_raw_fdb_key(adapter_opts, [source, @data_namespace, ""])
  end

  @doc """
  ## Examples

    iex> Ecto.Adapters.FoundationDB.Layer.Pack.to_raw_fdb_key([], ["table", "namespace", "id"])
    "\\xFEtable/namespace/id"

    iex> Ecto.Adapters.FoundationDB.Layer.Pack.to_raw_fdb_key([key_delimiter: <<0>>], ["table", "namespace", "id"])
    "\\xFEtable\\0namespace\\0id"

  """
  def to_raw_fdb_key(adapter_opts, list) when is_list(list) do
    to_raw_fdb_key(adapter_opts, @adapter_prefix, list)
  end

  def to_raw_fdb_key(adapter_opts, prefix, list) when is_list(list) do
    prefix <> Enum.join(list, Options.get(adapter_opts, :key_delimiter))
  end

  @doc """
  ## Examples

    iex> bin = Ecto.Adapters.FoundationDB.Layer.Pack.to_fdb_value([id: "my-pk-id", x: "a-value", y: 42])
    iex> Ecto.Adapters.FoundationDB.Layer.Pack.from_fdb_value(bin)
    [id: "my-pk-id", x: "a-value", y: 42]

  """
  def to_fdb_value(fields), do: :erlang.term_to_binary(fields)

  def from_fdb_value(bin), do: :erlang.binary_to_term(bin)

  def encode_pk_for_key(id), do: :erlang.term_to_binary(id)
end
