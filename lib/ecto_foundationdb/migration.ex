defmodule EctoFoundationDB.Migration do
  @moduledoc false

  alias EctoFoundationDB.Indexer.SchemaMetadata
  alias EctoFoundationDB.Migration.Index
  alias EctoFoundationDB.Schema

  @type adapter_meta :: Ecto.Adapter.adapter_meta()

  @type drop_mode :: :restrict | :cascade

  @typedoc "All migration commands"
  @type command ::
          raw ::
          String.t()
          | {:create, Index.t()}
          | {:create_if_not_exists, Index.t()}
          | {:drop, Index.t(), drop_mode()}
          | {:drop_if_exists, Index.t(), drop_mode()}

  @callback change() :: list(command())

  defmodule Index do
    @moduledoc """
    Used internally by adapters.

    To define an index in a migration, see `Ecto.Migration.index/3`.
    """
    defstruct schema: nil,
              prefix: nil,
              name: nil,
              columns: [],
              unique: false,
              concurrently: true,
              using: nil,
              include: [],
              only: false,
              nulls_distinct: nil,
              where: nil,
              comment: nil,
              options: []

    @type t :: %__MODULE__{
            schema: Ecto.Schema.t() | nil,
            prefix: atom,
            name: atom,
            columns: [atom | String.t()],
            unique: boolean,
            concurrently: boolean,
            using: atom | String.t(),
            only: boolean,
            include: [atom | String.t()],
            nulls_distinct: boolean | nil,
            where: atom | String.t(),
            comment: String.t() | nil,
            options: String.t()
          }
  end

  @doc false
  defmacro __using__(_) do
    quote location: :keep do
      import EctoFoundationDB.Migration
      @behaviour EctoFoundationDB.Migration
      @before_compile EctoFoundationDB.Migration
    end
  end

  @doc false
  defmacro __before_compile__(_env) do
    quote do
      def __migration__ do
        []
      end
    end
  end

  def create(index = %Index{}) do
    {:create, index}
  end

  def drop(index = %Index{}) do
    {:drop, index}
  end

  def index(schema, columns, opts \\ [])

  def index(schema, column, opts) when is_atom(schema) and is_atom(column) do
    index(schema, [column], opts)
  end

  def index(schema, columns, opts) when is_atom(schema) and is_list(columns) and is_list(opts) do
    validate_index_opts!(opts)
    index = struct(%Index{schema: schema, columns: columns}, opts)
    %{index | name: index.name || default_index_name(index)}
  end

  def metadata(schema, columns \\ [], opts \\ []) do
    options = opts[:options] || []
    options = Keyword.put(options, :indexer, SchemaMetadata)
    options = Keyword.put_new(options, :signals, SchemaMetadata.signal_names())
    opts = Keyword.put(opts, :options, options)
    validate_index_opts!(opts)
    index = struct(%Index{schema: schema, columns: columns}, opts)
    %{index | name: index.name || default_metadata_name(index)}
  end

  defp validate_index_opts!(opts), do: Keyword.validate!(opts, [:options])

  defp default_index_name(index) do
    default_name_(index, "index")
  end

  defp default_metadata_name(index) do
    default_name_(index, "metadata")
  end

  defp default_name_(index, label) do
    [Schema.get_source(index.schema), index.columns, label]
    |> List.flatten()
    |> Stream.map(&to_string(&1))
    |> Stream.map(&String.replace(&1, ~r"[^\w_]", "_"))
    |> Stream.map(&String.replace_trailing(&1, "_", ""))
    |> Enum.to_list()
    |> Enum.join("_")
    |> String.to_atom()
  end
end
