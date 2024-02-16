defmodule Ecto.Adapters.FoundationDB.QueryPlan do
  alias Ecto.Adapters.FoundationDB.Exception.Unsupported
  alias Ecto.Adapters.FoundationDB.Layer.Fields

  defmodule None do
    defstruct [:source, :is_pk?, :layer_data]
  end

  defmodule Equal do
    defstruct [:source, :field, :is_pk?, :param, :layer_data]
  end

  defmodule Between do
    defstruct [
      :source,
      :field,
      :is_pk?,
      :param_left,
      :param_right,
      :inclusive_left?,
      :inclusive_right?,
      :layer_data
    ]
  end

  def get(source, _schema, [], []) do
    %None{source: source, is_pk?: true, layer_data: %{}}
  end

  def get(
        source,
        schema,
        [
          %Ecto.Query.BooleanExpr{
            expr: {:==, [], [{{:., [], [{:&, [], [0]}, where_field]}, [], []}, {:^, [], [0]}]}
          }
        ],
        [param]
      ) do
    %Equal{
      source: source,
      field: where_field,
      is_pk?: Fields.get_pk_field!(schema) == where_field,
      param: param,
      layer_data: %{}
    }
  end

  def get(
        source,
        schema,
        [
          %Ecto.Query.BooleanExpr{
            op: :and,
            expr:
              {:and, [],
               [
                 {op_left, [],
                  [{{:., [], [{:&, [], [0]}, where_field_gt]}, [], []}, {:^, [], [0]}]},
                 {op_right, [],
                  [{{:., [], [{:&, [], [0]}, where_field_lt]}, [], []}, {:^, [], [1]}]}
               ]}
          }
        ],
        [param_left, param_right]
      )
      when where_field_gt == where_field_lt and
             (op_left == :> or op_left == :>=) and
             (op_right == :< or op_right == :<=) do
    %Between{
      source: source,
      field: where_field_gt,
      is_pk?: Fields.get_pk_field!(schema) == where_field_gt,
      param_left: param_left,
      param_right: param_right,
      inclusive_left?: op_left == :>=,
      inclusive_right?: op_right == :<=,
      layer_data: %{}
    }
  end

  def get(_source, _schema, wheres, _params) do
    raise Unsupported, """
    FoundationDB Adapater has not implemented support for your query

    #{inspect(wheres)}"
    """
  end
end
