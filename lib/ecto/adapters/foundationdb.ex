defmodule Ecto.Adapters.FoundationDB do
  @moduledoc """
  Documentation for `Ecto.Adapters.FoundationDB`.
  """

  @behaviour Ecto.Adapter
  @behaviour Ecto.Adapter.Schema
  @behaviour Ecto.Adapter.Queryable

  alias Ecto.Adapters.FoundationDB.EctoAdapter
  alias Ecto.Adapters.FoundationDB.EctoAdapterSchema
  alias Ecto.Adapters.FoundationDB.EctoAdapterQueryable

  def usetenant(struct, tenant) do
    Ecto.put_meta(struct, prefix: tenant)
  end

  @impl Ecto.Adapter
  defmacro __before_compile__(_env), do: :ok

  @impl Ecto.Adapter
  defdelegate ensure_all_started(config, type), to: EctoAdapter

  @impl Ecto.Adapter
  defdelegate init(config), to: EctoAdapter

  @impl Ecto.Adapter
  defdelegate checkout(data, config, fun), to: EctoAdapter

  @impl Ecto.Adapter
  defdelegate checked_out?(data), to: EctoAdapter

  @impl Ecto.Adapter
  defdelegate loaders(primitive_type, ecto_type), to: EctoAdapter

  @impl Ecto.Adapter
  defdelegate dumpers(primitive_type, ecto_type), to: EctoAdapter

  @impl Ecto.Adapter.Schema
  defdelegate autogenerate(type), to: EctoAdapterSchema

  @impl Ecto.Adapter.Schema
  defdelegate insert_all(
                adapter_meta,
                schema_meta,
                header,
                unsure,
                on_conflict,
                returning,
                placeholders,
                options
              ),
              to: EctoAdapterSchema

  @impl Ecto.Adapter.Schema
  defdelegate insert(adapter_meta, schema_meta, fields, on_conflict, returning, options),
    to: EctoAdapterSchema

  @impl Ecto.Adapter.Schema
  defdelegate update(adapter_meta, schema_meta, fields, filters, returning, options),
    to: EctoAdapterSchema

  @impl Ecto.Adapter.Schema
  defdelegate delete(adapter_meta, schema_meta, filters, returning, options),
    to: EctoAdapterSchema

  @impl Ecto.Adapter.Queryable
  defdelegate prepare(atom, query), to: EctoAdapterQueryable

  @impl Ecto.Adapter.Queryable
  defdelegate execute(adapter_meta, query_meta, query_cache, params, options),
    to: EctoAdapterQueryable

  @impl Ecto.Adapter.Queryable
  defdelegate stream(adapter_meta, query_meta, query_cache, params, options),
    to: EctoAdapterQueryable
end
