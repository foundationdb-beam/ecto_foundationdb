defmodule EctoFoundationDB.Layer.InternalMetadata do
  @moduledoc false
  def new(metadata, data) do
    {metadata, data}
  end

  def fetch({metadata, data}) do
    {:ok, {metadata, data}}
  end

  def fetch(obj) when is_list(obj) do
    :error
  end
end
