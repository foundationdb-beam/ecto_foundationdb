defmodule Ecto.Adapters.FoundationDB.Options do
  alias Ecto.Adapters.FoundationDB.Exception.Unsupported

  def get(options, :open_db), do: Keyword.get(options, :open_db, &:erlfdb.open/0)
  def get(options, :storage_id), do: Keyword.get(options, :storage_id, Ecto.Adapters.FoundationDB)
  def get(options, :key_delimiter), do: Keyword.get(options, :key_delimiter, "/")

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
