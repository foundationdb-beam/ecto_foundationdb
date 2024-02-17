defmodule Ecto.Adapters.FoundationDB.Layer.Query do
  alias Ecto.Adapters.FoundationDB.QueryPlan
  alias Ecto.Adapters.FoundationDB.Layer.IndexInventory
  alias Ecto.Adapters.FoundationDB.Layer.Tx
  alias Ecto.Adapters.FoundationDB.Layer.Pack
  alias Ecto.Adapters.FoundationDB.Exception.Unsupported

  def exec(db_or_tenant, adapter_meta, plan) do
    plan = make_range(db_or_tenant, adapter_meta, plan)

    db_or_tenant
    |> get_range(plan)
    |> unpack(plan)
    |> filter(plan)
  end

  defp make_range(db_or_tenant, adapter_meta, plan = %{layer_data: layer_data}) do
    if plan.is_pk? do
      make_datakey_range(adapter_meta, plan)
    else
      idxs = IndexInventory.get_for_source(adapter_meta, db_or_tenant, plan.source)

      case IndexInventory.select_index(idxs, [plan.field]) do
        {:ok, idx} ->
          make_indexkey_range(adapter_meta, %{plan | layer_data: Map.put(layer_data, :idx, idx)})

        {:error, _} ->
          raise Unsupported,
                """
                FoundationDB Adapter does not support a where clause constraining on a field other than the primary key or an index.
                """
      end
    end
  end

  defp get_range(db_or_tenant, %{layer_data: %{range: {fdb_key, nil}}}) do
    res = Tx.transactional(db_or_tenant, fn tx -> :erlfdb.wait(:erlfdb.get(tx, fdb_key)) end)

    if res == :not_found do
      []
    else
      [{fdb_key, res}]
    end
  end

  defp get_range(db_or_tenant, %{layer_data: %{range: {start_key, end_key}}}) do
    Tx.transactional(db_or_tenant, fn tx ->
      :erlfdb.wait(:erlfdb.get_range(tx, start_key, end_key))
    end)
  end

  defp unpack(kvs, %{layer_data: %{idx: _idx}}) do
    for {k, v} <- kvs, do: {k, Pack.from_fdb_value(v)[:value]}
  end

  defp unpack(kvs, _plan) do
    for {k, v} <- kvs, do: {k, Pack.from_fdb_value(v)}
  end

  defp filter(kvs, %QueryPlan.None{}) do
    kvs
  end

  defp filter(kvs, plan = %QueryPlan.Equal{layer_data: %{idx: _idx}}) do
    Enum.filter(kvs, fn {_key, data_object} ->
      # Filter by the where_values because our indexes can have key conflicts
      plan.param == data_object[plan.field]
    end)
  end

  defp filter(kvs, %QueryPlan.Between{layer_data: %{idx: _idx}}) do
    # Between on non-timeseries index is not supported, and between on
    # time series index will not have conflicts, so no filtering needed
    kvs
  end

  defp filter(kvs, _plan) do
    kvs
  end

  defp make_datakey_range(%{opts: adapter_opts}, plan = %QueryPlan.None{layer_data: layer_data}) do
    start_key = Pack.to_fdb_datakey(adapter_opts, plan.source, "")
    end_key = :erlfdb_key.strinc(start_key)
    %QueryPlan.None{plan | layer_data: Map.put(layer_data, :range, {start_key, end_key})}
  end

  defp make_datakey_range(
         %{opts: adapter_opts},
         plan = %QueryPlan.Equal{param: param, layer_data: layer_data}
       ) do
    fdb_key = Pack.to_fdb_datakey(adapter_opts, plan.source, param)
    %QueryPlan.Equal{plan | layer_data: Map.put(layer_data, :range, {fdb_key, nil})}
  end

  defp make_datakey_range(
         %{opts: adapter_opts},
         plan = %QueryPlan.Between{
           param_left: param_left,
           param_right: param_right,
           layer_data: layer_data
         }
       ) do
    start_key = Pack.to_fdb_datakey(adapter_opts, plan.source, param_left)
    end_key = Pack.to_fdb_datakey(adapter_opts, plan.source, param_right)

    start_key = if plan.inclusive_left?, do: start_key, else: :erlfdb_key.strinc(start_key)
    end_key = if plan.inclusive_right?, do: :erlfdb_key.strinc(end_key), else: end_key
    %QueryPlan.Between{plan | layer_data: Map.put(layer_data, :range, {start_key, end_key})}
  end

  defp make_indexkey_range(_adapter_meta, %QueryPlan.None{}) do
    raise Unsupported, """
    FoundationDB Adapter does not support empty where clause on an index. In fact, this code path should not be reachable.
    """
  end

  defp make_indexkey_range(
         %{opts: adapter_opts},
         plan = %QueryPlan.Equal{
           layer_data: layer_data = %{idx: idx}
         }
       ) do
    start_key =
      Pack.to_fdb_indexkey(
        adapter_opts,
        idx[:options],
        plan.source,
        idx[:id],
        [plan.param],
        ""
      )

    end_key = :erlfdb_key.strinc(start_key)
    %QueryPlan.Equal{plan | layer_data: Map.put(layer_data, :range, {start_key, end_key})}
  end

  defp make_indexkey_range(
         %{opts: adapter_opts},
         plan = %QueryPlan.Between{
           layer_data: layer_data = %{idx: idx}
         }
       ) do
    index_options = idx[:options]

    if Keyword.get(index_options, :timeseries, false) do
      [start_key, end_key] =
        for x <- [plan.param_left, plan.param_right],
            do:
              Pack.to_fdb_indexkey(
                adapter_opts,
                idx[:options],
                plan.source,
                idx[:id],
                [x],
                ""
              )

      %QueryPlan.Between{plan | layer_data: Map.put(layer_data, :range, {start_key, end_key})}
    else
      raise Unsupported, """
      FoundationDB Adapter does not support 'between' queries on indexes that are not timeseries.
      """
    end
  end
end
