defmodule EctoFoundationDB.Schema do
  @moduledoc false
  def get_context!(_source, schema) do
    %{__meta__: _meta = %{context: context}} = Kernel.struct!(schema)
    context
  end

  def get_source(schema) do
    schema.__schema__(:source)
  end

  def field_types(schema) do
    field_types(schema, schema.__schema__(:fields))
  end

  def field_types(schema, fields) do
    for field <- fields,
        do: {field, schema.__schema__(:type, field)}
  end

  def get_option(context, :usetenant), do: get_option(context, :usetenant, false)
  def get_option(context, :write_primary), do: get_option(context, :write_primary, true)

  def get_option(nil, _key, default), do: default
  def get_option(context, key, default), do: Keyword.get(context, key, default)
end
