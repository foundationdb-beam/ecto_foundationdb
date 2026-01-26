defmodule EctoFoundationDB.Layer.Ordering do
  @moduledoc false

  def get_ordering([]), do: []
  def get_ordering(nil), do: []

  def get_ordering(order_bys) do
    parse_ordering(order_bys)
  end

  defp parse_ordering(ordering) do
    ordering
    |> join_exprs()
    |> Enum.map(fn {dir, {{:., [], [{:&, [], [0]}, field]}, _, _}} ->
      {dir, field}
    end)
  end

  defp join_exprs([%{expr: exprs1}, %{expr: exprs2} | t]) do
    join_exprs([%{expr: Keyword.merge(exprs1, exprs2)} | t])
  end

  defp join_exprs([%{expr: exprs1}]), do: exprs1
end
