defmodule Ecto.Adapters.FoundationDB.Schema do
  @moduledoc """
  This internal module deals with options on the Ecto Schema.
  """
  def get_context!(_source, schema) do
    %{__meta__: _meta = %{context: context}} = Kernel.struct!(schema)
    context
  end

  def get_option(context, :usetenant), do: get_option(context, :usetenant, false)
  def get_option(context, :write_primary), do: get_option(context, :write_primary, true)

  def get_option(nil, _key, default), do: default
  def get_option(context, key, default), do: Keyword.get(context, key, default)
end
