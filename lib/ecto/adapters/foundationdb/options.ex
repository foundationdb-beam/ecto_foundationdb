defmodule Ecto.Adapters.FoundationDB.Options do
  alias Ecto.Adapters.FoundationDB.Exception.Unsupported

  def get(options, key), do: get_or_raise(options, key, "FoundationDB Adapter does not specify a default for option #{inspect(key)}")

  defp get_or_raise(options, key, message) do
    case options[key] do
      nil ->
        raise Unsupported, message
      val ->
        val
    end
  end
end
