defmodule Ecto.Adapters.FoundationDB.QueryPlan do
  @moduledoc """
  This internal module parses Ecto Query into simpler constructs.

  The FoundationDB Adapter does not support the full Query API.
  """
  alias Ecto.Adapters.FoundationDB.Exception.Unsupported
  alias Ecto.Adapters.FoundationDB.Layer.Fields

  defmodule None do
    @moduledoc false
    defstruct [:source, :schema, :context, :is_pk?, :updates, :layer_data]
  end

  defmodule Equal do
    @moduledoc false
    defstruct [:source, :schema, :context, :field, :is_pk?, :param, :updates, :layer_data]
  end

  defmodule Between do
    @moduledoc false
    defstruct [
      :source,
      :schema,
      :context,
      :field,
      :is_pk?,
      :param_left,
      :param_right,
      :inclusive_left?,
      :inclusive_right?,
      :updates,
      :layer_data
    ]
  end

  def get(source, schema, context, [], _updates, []) do
    %None{
      source: source,
      schema: schema,
      context: context,
      is_pk?: true,
      updates: [],
      layer_data: %{}
    }
  end

  def get(
        source,
        schema,
        context,
        [
          %Ecto.Query.BooleanExpr{
            expr:
              {:==, [],
               [{{:., [], [{:&, [], [0]}, where_field]}, [], []}, {:^, [], [param_index]}]}
          }
        ],
        updates,
        params
      ) do
    %Equal{
      source: source,
      schema: schema,
      context: context,
      field: where_field,
      is_pk?: Fields.get_pk_field!(schema) == where_field,
      param: Enum.at(params, param_index),
      updates: resolve_updates(updates, params),
      layer_data: %{}
    }
  end

  def get(
        source,
        schema,
        context,
        [
          %Ecto.Query.BooleanExpr{
            op: :and,
            expr:
              {:and, [],
               [
                 {op_left, [],
                  [
                    {{:., [], [{:&, [], [0]}, where_field_gt]}, [], []},
                    {:^, [], [param_index_left]}
                  ]},
                 {op_right, [],
                  [
                    {{:., [], [{:&, [], [0]}, where_field_lt]}, [], []},
                    {:^, [], [param_index_right]}
                  ]}
               ]}
          }
        ],
        updates,
        params
      )
      when where_field_gt == where_field_lt and
             (op_left == :> or op_left == :>=) and
             (op_right == :< or op_right == :<=) do
    %Between{
      source: source,
      schema: schema,
      context: context,
      field: where_field_gt,
      is_pk?: Fields.get_pk_field!(schema) == where_field_gt,
      param_left: Enum.at(params, param_index_left),
      param_right: Enum.at(params, param_index_right),
      inclusive_left?: op_left == :>=,
      inclusive_right?: op_right == :<=,
      updates: resolve_updates(updates, params),
      layer_data: %{}
    }
  end

  def get(_source, _schema, _context, wheres, _updates, _params) do
    raise Unsupported, """
    FoundationDB Adapter has not implemented support for your query

    #{inspect(wheres)}"
    """
  end

  defp resolve_updates([%Ecto.Query.QueryExpr{expr: [set: pins]}], params) do
    field_vals =
      for {field, {:^, [], [param_index]}} <- pins do
        {field, Enum.at(params, param_index)}
      end

    [set: field_vals]
  end

  defp resolve_updates([%Ecto.Query.QueryExpr{expr: [_ | _]}], _params) do
    raise Unsupported, """
    FoundationDB Adapter does not support your update operation.
    """
  end

  defp resolve_updates([], _params) do
    []
  end
end
