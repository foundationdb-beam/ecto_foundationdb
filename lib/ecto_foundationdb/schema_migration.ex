defmodule EctoFoundationDB.Migration.SchemaMigration do
  # Defines a schema that works with a table that tracks schema migrations.
  @moduledoc false
  alias EctoFoundationDB.Layer.DecodedKV
  alias EctoFoundationDB.Layer.Metadata
  alias EctoFoundationDB.Layer.Query
  alias EctoFoundationDB.Layer.Tx
  alias EctoFoundationDB.QueryPlan
  use Ecto.Schema

  @schema_migrations_source "schema_migrations"

  def source(), do: @schema_migrations_source

  @primary_key false
  schema @schema_migrations_source do
    field(:version, :integer, primary_key: true)
    timestamps(updated_at: false)
  end

  def up(tenant, version) do
    source = @schema_migrations_source
    inserted_at = NaiveDateTime.utc_now() |> NaiveDateTime.truncate(:second)
    data_object = [version: version, inserted_at: inserted_at]
    entry = {{:version, version}, data_object}

    metadata = Metadata.builtin_metadata(source)

    count =
      Tx.transactional(tenant, fn tx ->
        Tx.insert_all(
          tenant,
          tx,
          {__MODULE__, source, []},
          [entry],
          metadata,
          []
        )
      end)

    {:ok, count}
  end

  def get_max_versions(tenant) do
    source = @schema_migrations_source

    plan = %QueryPlan{
      tenant: tenant,
      source: source,
      schema: __MODULE__,
      context: [],
      constraints: [%QueryPlan.None{pk?: true}],
      updates: [],
      layer_data: %Query{},
      ordering: [%QueryPlan.Order{field: :version, pk?: true, monotonicity: :desc}],
      limit: nil
    }

    Tx.transactional(tenant, fn _tx ->
      {iterator, _post_ordering} = Query.all(tenant, %{}, plan, key_limit: 1)

      iterator
      |> FDB.Stream.from_iterator()
      |> Stream.map(fn %DecodedKV{data_object: obj} -> obj[:version] end)
      |> Enum.take(1)
    end)
  end
end
