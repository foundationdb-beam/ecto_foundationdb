defmodule Ecto.Adapters.FoundationDB.Schema do
  alias Ecto.Adapters.FoundationDB.EctoAdapterMigration

  def get_context!(source, nil) do
    if EctoAdapterMigration.is_migration_source?(source) do
      EctoAdapterMigration.get_context(source)
    else
      nil
    end
  end

  def get_context!(_source, schema) do
    %{__meta__: _meta = %{context: context}} = Kernel.struct!(schema)
    context
  end
end
