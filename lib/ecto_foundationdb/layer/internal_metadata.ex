defmodule EctoFoundationDB.Layer.InternalMetadata do
  @moduledoc false
  @magic_key :__ectofdb_internal_metadata__
  def new(module) do
    [{@magic_key, module}]
  end

  def fetch(obj) when is_list(obj) do
    case Keyword.fetch(obj, @magic_key) do
      {:ok, val} ->
        {true, val}

      :error ->
        {false, nil}
    end
  end
end
