defmodule EctoFoundationDB.Layer.Ordering do
  alias EctoFoundationDB.QueryPlan
  alias EctoFoundationDB.Schema

  @moduledoc false
  # This module emulates `query.order_bys` behavior, because FoundationDB
  # doesn't have native support for result ordering.

  @doc """
  Returns the ordering function that needs to be applied on a query result.
  """
  def get_post_query_ordering_fn(_schema, []), do: nil
  def get_post_query_ordering_fn(_schema, nil), do: nil

  def get_post_query_ordering_fn(schema, ordering) do
    field_types = if is_nil(schema), do: %{}, else: Schema.field_types(schema) |> Enum.into(%{})

    fun = fn data ->
      Enum.sort(data, &sort(&1, &2, field_types, ordering))
    end

    fun
  end

  defp sort(left, right, field_types, ordering) do
    v = cmp(left, right, field_types, ordering, :lt)
    v in [:lt, :eq]
  end

  defp cmp(_, _, _, [], default), do: default

  defp cmp(
         left,
         right,
         field_types,
         [%QueryPlan.Order{monotonicity: :asc, field: field} | t],
         _default
       ) do
    case cmp_field(field_types[field], left[field], right[field]) do
      :eq ->
        cmp(left, right, field_types, t, :eq)

      cmp_res ->
        cmp_res
    end
  end

  defp cmp(
         left,
         right,
         field_types,
         [%QueryPlan.Order{monotonicity: :desc, field: field} | t],
         _default
       ) do
    case cmp_field(field_types[field], left[field], right[field]) do
      :eq ->
        cmp(left, right, field_types, t, :eq)

      cmp_res ->
        reverse_cmp(cmp_res)
    end
  end

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
