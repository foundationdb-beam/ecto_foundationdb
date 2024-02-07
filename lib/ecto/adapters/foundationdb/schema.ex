defmodule Ecto.Adapters.FoundationDB.Schema do
  def get_context!(schema) do
    %{__meta__: %{context: context}} = Kernel.struct!(schema)
    context
  end
end
