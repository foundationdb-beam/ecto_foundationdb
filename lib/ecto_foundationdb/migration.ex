defmodule EctoFoundationDB.Migration do
  @moduledoc false

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

  @typedoc """
  A struct that represents a table or index in a database schema.

  These database objects can be modified through the use of a Data
  Definition Language, hence the name DDL object.
  """
  @type ddl_object :: Index.t()

  @doc """
  Checks if the adapter supports ddl transaction.
  """
  @callback supports_ddl_transaction? :: boolean

  @doc """
  Executes migration commands.
  """
  @callback execute_ddl(adapter_meta, command, options :: Keyword.t()) ::
              {:ok, [{Logger.level(), Logger.message(), Logger.metadata()}]}

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

  def index(schema, columns, opts \\ [])

  def index(schema, column, opts) when is_atom(schema) and is_atom(column) do
    index(schema, [column], opts)
  end

  def index(schema, columns, opts) when is_atom(schema) and is_list(columns) and is_list(opts) do
    validate_index_opts!(opts)
    index = struct(%Index{schema: schema, columns: columns}, opts)
    %{index | name: index.name || default_index_name(index)}
  end

  defp validate_index_opts!(opts), do: Keyword.validate!(opts, [:options])

  defp default_index_name(index) do
    [Schema.get_source(index.schema), index.columns, "index"]
    |> List.flatten()
    |> Stream.map(&to_string(&1))
    |> Stream.map(&String.replace(&1, ~r"[^\w_]", "_"))
    |> Stream.map(&String.replace_trailing(&1, "_", ""))
    |> Enum.to_list()
    |> Enum.join("_")
    |> String.to_atom()
  end
end
