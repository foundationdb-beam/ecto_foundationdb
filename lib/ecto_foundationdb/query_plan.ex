defmodule EctoFoundationDB.QueryPlan do
  @moduledoc """
  Internal module only used if you are implementing a custom index.
  """
  alias EctoFoundationDB.Exception.Unsupported
  alias EctoFoundationDB.Layer.Fields
  alias EctoFoundationDB.QueryPlan.Between
  alias EctoFoundationDB.QueryPlan.Equal
  alias EctoFoundationDB.QueryPlan.None

  defstruct [:source, :schema, :context, :constraints, :updates, :layer_data]

  @type t() :: %__MODULE__{}

  defmodule None do
    @moduledoc false
    defstruct [:is_pk?]
  end

  defmodule Equal do
    @moduledoc false
    defstruct [:field, :is_pk?, :param]
  end

  defmodule Between do
    @moduledoc false
    defstruct [
      :field,
      :is_pk?,
      :param_left,
      :param_right,
      :inclusive_left?,
      :inclusive_right?
    ]
  end

  def get(source, schema, context, wheres, updates, params) do
    constraints =
      case walk_ast(wheres, schema, params, []) do
        [] ->
          [%None{is_pk?: true}]

        list ->
          list
      end

    %__MODULE__{
      source: source,
      schema: schema,
      context: context,
      constraints:
        Enum.sort(constraints, fn
          %Equal{}, %Between{} -> true
          _, _ -> false
        end),
      updates: resolve_updates(updates, params),
      layer_data: %{}
    }
  end

  def walk_ast([], _schema, _params, constraints) do
    Enum.reverse(constraints)
  end

  def walk_ast(
        [%Ecto.Query.BooleanExpr{op: :and, expr: {:and, [], [lhs, rhs]}} | rest],
        schema,
        params,
        constraints
      ) do
    case get_op({lhs, rhs}, schema, params) do
      nil ->
        constraints_lhs =
          walk_ast([%Ecto.Query.BooleanExpr{op: :and, expr: lhs}], schema, params, [])

        constraints_rhs =
          walk_ast([%Ecto.Query.BooleanExpr{op: :and, expr: rhs}], schema, params, [])

        constraints = constraints_rhs ++ constraints_lhs ++ constraints
        walk_ast(rest, schema, params, constraints)

      op ->
        walk_ast(rest, schema, params, [op | constraints])
    end
  end

  def walk_ast(
        wheres = [%Ecto.Query.BooleanExpr{expr: expr} | rest],
        schema,
        params,
        constraints
      ) do
    case get_op(expr, schema, params) do
      nil ->
        raise Unsupported, """
        FoundationDB Adapter has not implemented support for your query

        #{inspect(wheres)}"
        """

      op ->
        walk_ast(rest, schema, params, [op | constraints])
    end
  end

  def walk_ast(wheres, _schema, _params, _constraints) do
    raise Unsupported, """
    FoundationDB Adapter has not implemented support for your query

    #{inspect(wheres)}"
    """
  end

  def get_op(
        {:==, [], [{{:., [], [{:&, [], [0]}, where_field]}, [], []}, where_param]},
        schema,
        params
      ) do
    %Equal{
      field: where_field,
      is_pk?: Fields.get_pk_field!(schema) == where_field,
      param: get_pinned_param(params, where_param)
    }
  end

  def get_op(
        {
          {op_left, [],
           [
             {{:., [], [{:&, [], [0]}, where_field_gt]}, [], []},
             where_param_left
           ]},
          {op_right, [],
           [
             {{:., [], [{:&, [], [0]}, where_field_lt]}, [], []},
             where_param_right
           ]}
        },
        schema,
        params
      )
      when where_field_gt == where_field_lt and
             (op_left == :> or op_left == :>=) and
             (op_right == :< or op_right == :<=) do
    %Between{
      field: where_field_gt,
      is_pk?: Fields.get_pk_field!(schema) == where_field_gt,
      param_left: get_pinned_param(params, where_param_left),
      param_right: get_pinned_param(params, where_param_right),
      inclusive_left?: op_left == :>=,
      inclusive_right?: op_right == :<=
    }
  end

  def get_op(_, _schema, _params) do
    nil
  end

  def get_pinned_param(params, {:^, [], [pos]}) do
    Enum.at(params, pos)
  end

  def get_pinned_param(_params, val) do
    val
  end

  defp resolve_updates([%Ecto.Query.QueryExpr{expr: [set: pins]}], params) do
    field_vals =
      for {field, {:^, [], [param_pos]}} <- pins do
        {field, Enum.at(params, param_pos)}
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
