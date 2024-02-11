defmodule Ecto.Adapters.FoundationDB.Schema do
  def get_context!(nil), do: nil

  def get_context!(schema) do
    %{__meta__: _meta = %{context: context}} = Kernel.struct!(schema)
    context
  end
end
