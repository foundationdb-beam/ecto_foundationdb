defmodule EctoFoundationDB.Layer.Ordering do
  alias EctoFoundationDB.Schema

  @moduledoc false
  # This module emulates `query.order_bys` behavior, because FoundationDB
  # doesn't have native support for result ordering.

  @doc """
  Returns the ordering function that needs to be applied on a query result.
  """
  def get_ordering_fn(_schema, []), do: {[], &Function.identity/1}
  def get_ordering_fn(_schema, nil), do: {[], &Function.identity/1}

  def get_ordering_fn(schema, ordering) do
    field_types = if is_nil(schema), do: %{}, else: Schema.field_types(schema) |> Enum.into(%{})
    parsed_ordering = parse_ordering(ordering)

    fun = fn data ->
      Enum.sort(data, &sort(&1, &2, field_types, parsed_ordering))
    end

    {parsed_ordering, fun}
  end

  defp parse_ordering(ordering) do
    ordering
    |> join_exprs()
    |> Enum.map(fn {dir, {{:., [], [{:&, [], [0]}, field]}, _, _}} ->
      {dir, field}
    end)
  end

  defp sort(left, right, field_types, ordering) do
    cmp(left, right, field_types, ordering) == :lt
  end

  defp join_exprs([%{expr: exprs1}, %{expr: exprs2} | t]) do
    join_exprs([%{expr: Keyword.merge(exprs1, exprs2)} | t])
  end

  defp join_exprs([%{expr: exprs1}]), do: exprs1

  defp cmp(left, right, field_types, [{:asc, field} | t]) do
    case cmp_field(field_types[field], left[field], right[field]) do
      :eq ->
        cmp(left, right, field_types, t)

      cmp_res ->
        cmp_res
    end
  end

  defp cmp(left, right, field_types, [{:desc, field} | t]) do
    case cmp_field(field_types[field], left[field], right[field]) do
      :eq ->
        cmp(left, right, field_types, t)

      cmp_res ->
        reverse_cmp(cmp_res)
    end
  end

  defp cmp(_, _, _, []), do: :gt

  if Code.ensure_loaded?(Decimal) do
    defp cmp_field(:decimal, lhs, rhs), do: Decimal.compare(lhs, rhs)
  end

  defp cmp_field(:utc_datetime, lhs, rhs), do: DateTime.compare(lhs, rhs)
  defp cmp_field(:naive_datetime, lhs, rhs), do: NaiveDateTime.compare(lhs, rhs)
  defp cmp_field(:date, lhs, rhs), do: Date.compare(lhs, rhs)
  defp cmp_field(:time, lhs, rhs), do: Time.compare(lhs, rhs)
  defp cmp_field(:utc_datetime_usec, lhs, rhs), do: DateTime.compare(lhs, rhs)
  defp cmp_field(:naive_datetime_usec, lhs, rhs), do: NaiveDateTime.compare(lhs, rhs)
  defp cmp_field(:time_usec, lhs, rhs), do: Time.compare(lhs, rhs)

  defp cmp_field(_, lhs, rhs) do
    cond do
      lhs < rhs ->
        :lt

      lhs > rhs ->
        :gt

      true ->
        :eq
    end
  end

  defp reverse_cmp(:gt), do: :lt
  defp reverse_cmp(:lt), do: :gt
end
