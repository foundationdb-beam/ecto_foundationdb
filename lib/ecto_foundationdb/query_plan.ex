defmodule EctoFoundationDB.QueryPlan do
  @moduledoc "See `Ecto.Adapters.FoundationDB`"
  alias EctoFoundationDB.Exception.Unsupported
  alias EctoFoundationDB.Layer.Fields
  alias EctoFoundationDB.QueryPlan.Between
  alias EctoFoundationDB.QueryPlan.Equal
  alias EctoFoundationDB.QueryPlan.None
  alias EctoFoundationDB.QueryPlan.Order

  @enforce_keys [
    :tenant,
    :source,
    :schema,
    :context,
    :constraints,
    :updates,
    :layer_data,
    :ordering,
    :limit
  ]
  defstruct @enforce_keys

  @type t() :: %__MODULE__{}

  defmodule None do
    @moduledoc false
    defstruct [:pk?, :fields]
  end

  defmodule Equal do
    @moduledoc false
    defstruct [:field, :pk?, :param]
  end

  defmodule Between do
    @moduledoc false
    defstruct [
      :field,
      :pk?,
      :param_left,
      :param_right,
      :inclusive_left?,
      :inclusive_right?
    ]
  end

  defmodule Order do
    @moduledoc false
    defstruct [
      :field,
      :pk?,
      :monotonicity
    ]
  end

  def all_range(tenant, source, schema, context, id_s, id_e, options) do
    %__MODULE__{
      tenant: tenant,
      source: source,
      schema: schema,
      context: context,
      constraints: [
        %Between{
          field: :_,
          pk?: true,
          param_left: id_s,
          param_right: id_e,
          inclusive_left?: is_nil(id_s) || Keyword.get(options, :inclusive_left?, true),
          inclusive_right?: is_nil(id_e) || Keyword.get(options, :inclusive_right?, false)
        }
      ],
      updates: [],
      layer_data: %{},
      ordering: [],
      limit: Keyword.get(options, :limit)
    }
  end

  def get(tenant, source, schema, context, wheres, updates, params, order_bys, limit) do
    ordering = parse_order_bys(schema, order_bys)

    constraints =
      case walk_ast(wheres, schema, params, []) do
        [] ->
          none_c = make_none_constraint(ordering)
          [none_c]

        list ->
          list
      end

    %__MODULE__{
      tenant: tenant,
      source: source,
      schema: schema,
      context: context,
      constraints:
        merge_betweens(
          Enum.sort(constraints, fn
            %Equal{}, %Between{} -> true
            _, _ -> false
          end)
        ),
      updates: resolve_updates(updates, params),
      layer_data: %{},
      ordering: ordering,
      limit: limit
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
      pk?: pk?(schema, where_field),
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
      pk?: pk?(schema, where_field_gt),
      param_left: get_pinned_param(params, where_param_left),
      param_right: get_pinned_param(params, where_param_right),
      inclusive_left?: op_left == :>=,
      inclusive_right?: op_right == :<=
    }
  end

  def get_op(
        {op, [], [{{:., [], [{:&, [], [0]}, where_field]}, [], []}, where_param]},
        schema,
        params
      )
      when op in ~w[> >=]a do
    %Between{
      field: where_field,
      pk?: pk?(schema, where_field),
      param_left: get_pinned_param(params, where_param),
      param_right: nil,
      inclusive_left?: op == :>=,
      inclusive_right?: true
    }
  end

  def get_op(
        {op, [], [{{:., [], [{:&, [], [0]}, where_field]}, [], []}, where_param]},
        schema,
        params
      )
      when op in ~w[< <=]a do
    %Between{
      field: where_field,
      pk?: pk?(schema, where_field),
      param_left: nil,
      param_right: get_pinned_param(params, where_param),
      inclusive_left?: true,
      inclusive_right?: op == :<=
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

  defp pk?(nil, _param), do: nil

  defp pk?(schema, param) do
    Fields.get_pk_field!(schema) == param
  end

  def parse_order_bys(_schema, []), do: []

  def parse_order_bys(schema, order_bys) do
    order_bys
    |> join_exprs()
    |> Enum.map(fn {dir, {{:., [], [{:&, [], [0]}, field]}, _, _}} ->
      %Order{field: field, pk?: pk?(schema, field), monotonicity: dir}
    end)
  end

  defp join_exprs([%{expr: exprs1}, %{expr: exprs2} | t]) do
    join_exprs([%{expr: Keyword.merge(exprs1, exprs2)} | t])
  end

  defp join_exprs([%{expr: exprs1}]), do: exprs1

  defp make_none_constraint([]), do: %None{pk?: true}

  defp make_none_constraint([%Order{pk?: true, field: field} | _tail_ordering]),
    do: %None{pk?: true, fields: [field]}

  defp make_none_constraint(ordering) do
    fields = for %Order{field: field} <- ordering, do: field
    %None{pk?: false, fields: fields}
  end

  defp merge_betweens(constraints) do
    betweens = for(b = %Between{} <- constraints, do: b)

    to_merge =
      case betweens do
        [a = %{field: field, param_right: nil}, b = %{field: field, param_left: nil}] ->
          {a, b}

        [a = %{field: field, param_left: nil}, b = %{field: field, param_right: nil}] ->
          {b, a}

        _ ->
          nil
      end

    case to_merge do
      nil ->
        constraints

      {l, r} ->
        non_betweens = constraints -- betweens

        non_betweens ++
          [
            %Between{
              field: l.field,
              pk?: l.pk?,
              param_left: l.param_left,
              param_right: r.param_right,
              inclusive_left?: l.inclusive_left?,
              inclusive_right?: r.inclusive_right?
            }
          ]
    end
  end
end
