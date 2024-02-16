defmodule Ecto.Adapters.FoundationDB.Options do
  alias Ecto.Adapters.FoundationDB.Exception.Unsupported
  alias Ecto.Adapters.FoundationDB.Layer.Pack

  def get(options, :open_db) do
    Keyword.get(options, :open_db, fn -> :erlfdb.open(get(options, :cluster_file)) end)
  end

  def get(options, :storage_id),
    do: Keyword.get(options, :storage_id, "Ecto.Adapters.FoundationDB")

  def get(options, :key_delimiter), do: Keyword.get(options, :key_delimiter, "/")

  def get(options, :indexkey_encoder),
    do: Keyword.get(options, :indexkey_encoder, &Pack.indexkey_encoder/2)

  def get(options, :cluster_file), do: Keyword.get(options, :cluster_file, "")

  def get(options, key),
    do:
      get_or_raise(
        options,
        key,
        "FoundationDB Adapter does not specify a default for option #{inspect(key)}"
      )

  defp get_or_raise(options, key, message) do
    case options[key] do
      nil ->
        raise Unsupported, message

      val ->
        val
    end
  end
end
