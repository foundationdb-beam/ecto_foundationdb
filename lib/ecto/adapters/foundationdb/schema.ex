defmodule Ecto.Adapters.FoundationDB.Schema do
  def get_context!(_source, schema) do
    %{__meta__: _meta = %{context: context}} = Kernel.struct!(schema)
    context
  end
end
