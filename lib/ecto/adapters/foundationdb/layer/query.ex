defmodule Ecto.Adapters.FoundationDB.Layer.Query do
  @moduledoc """
  This internal module handles execution of Ecto Query requests.
  """
  alias Ecto.Adapters.FoundationDB.Exception.Unsupported
  alias Ecto.Adapters.FoundationDB.Layer.Fields
  alias Ecto.Adapters.FoundationDB.Layer.Indexer
  alias Ecto.Adapters.FoundationDB.Layer.IndexInventory
  alias Ecto.Adapters.FoundationDB.Layer.Pack
  alias Ecto.Adapters.FoundationDB.Layer.Tx
  alias Ecto.Adapters.FoundationDB.QueryPlan
  alias Ecto.Adapters.FoundationDB.Schema

  defmodule Continuation do
    @moduledoc false
    defstruct more?: false, start_key: nil
  end

  @doc """
  Executes a query for retrieving data.
  """
  def all(db_or_tenant, adapter_meta, plan, options \\ []) do
    {plan, kvs} =
      IndexInventory.transactional(db_or_tenant, adapter_meta, plan.source, fn tx, idxs ->
        plan = make_range(idxs, adapter_meta, plan, options)
        {plan, tx_get_range(tx, plan, options)}
      end)

    continuation = continuation(kvs, options)

    objs =
      kvs
      |> unpack_and_filter(plan)
      |> Stream.map(fn {_k, v} -> v end)

    {objs, continuation}
  end

  @doc """
  Executes a query for updating data.
  """
  def update(db_or_tenant, adapter_meta, plan) do
    IndexInventory.transactional(db_or_tenant, adapter_meta, plan.source, fn tx, idxs ->
      plan = make_range(idxs, adapter_meta, plan, [])
      tx_update_range(tx, adapter_meta, plan, idxs)
    end)
  end

  @doc """
  Executes a query for deleting data.
  """
  def delete(db_or_tenant, adapter_meta, plan = %QueryPlan.None{}) do
    # Special case, very efficient
    Tx.transactional(db_or_tenant, &Tx.clear_all(&1, adapter_meta, plan.source))
  end

  def delete(db_or_tenant, adapter_meta, plan) do
    IndexInventory.transactional(db_or_tenant, adapter_meta, plan.source, fn tx, idxs ->
      plan = make_range(idxs, adapter_meta, plan, [])
      tx_delete_range(tx, adapter_meta, plan, idxs)
    end)
  end

  defp make_range(idxs, adapter_meta, plan = %{layer_data: layer_data}, options) do
    if plan.is_pk? do
      make_datakey_range(adapter_meta, plan, options)
    else
      case IndexInventory.select_index(idxs, [plan.field]) do
        {:ok, idx} ->
          range = Indexer.range(idx, adapter_meta, plan, options)

          layer_data =
            layer_data
            |> Map.put(:idx, idx)
            |> Map.put(:range, range)

          %{plan | layer_data: layer_data}

        {:error, _} ->
          raise Unsupported,
                """
                FoundationDB Adapter does not support a where clause constraining on a field other than the primary key or an index.
                """
      end
    end
  end

  defp tx_get_range(tx, %{layer_data: %{range: {fdb_key, nil}}}, _options) do
    res = :erlfdb.wait(:erlfdb.get(tx, fdb_key))

    if res == :not_found do
      []
    else
      [{fdb_key, res}]
    end
  end

  defp tx_get_range(tx, %{layer_data: %{range: {start_key, end_key}}}, options) do
    get_options = Keyword.take(options, [:limit])
    :erlfdb.wait(:erlfdb.get_range(tx, start_key, end_key, get_options))
  end

  defp tx_update_range(tx, adapter_meta, plan = %{updates: updates}, idxs) do
    pk_field = Fields.get_pk_field!(plan.schema)
    write_primary = Schema.get_option(plan.context, :write_primary)

    tx
    |> tx_get_range(plan, [])
    |> unpack_and_filter(plan)
    |> Stream.map(
      &Tx.update_data_object(
        tx,
        adapter_meta,
        pk_field,
        &1,
        updates,
        idxs,
        write_primary
      )
    )
    |> Enum.to_list()
    |> length()
  end

  defp tx_delete_range(tx, adapter_meta, plan, idxs) do
    tx
    |> tx_get_range(plan, [])
    |> unpack_and_filter(plan)
    |> Stream.map(&Tx.delete_data_object(tx, adapter_meta, &1, idxs))
    |> Enum.to_list()
    |> length()
  end

  defp unpack_and_filter(kvs, plan = %{layer_data: %{idx: idx}}) do
    kvs
    |> Stream.map(&Indexer.unpack(idx, plan, &1))
    |> Stream.filter(fn
      nil -> false
      _ -> true
    end)
  end

  defp unpack_and_filter(kvs, _plan) do
    Stream.map(kvs, fn {k, v} -> {k, Pack.from_fdb_value(v)} end)
  end

  defp make_datakey_range(
         %{opts: adapter_opts},
         plan = %QueryPlan.None{layer_data: layer_data},
         options
       ) do
    start_key = Pack.to_fdb_datakey_startswith(adapter_opts, plan.source)
    end_key = :erlfdb_key.strinc(start_key)
    start_key = options[:start_key] || start_key
    %QueryPlan.None{plan | layer_data: Map.put(layer_data, :range, {start_key, end_key})}
  end

  defp make_datakey_range(
         %{opts: adapter_opts},
         plan = %QueryPlan.Equal{param: param, layer_data: layer_data},
         _options
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
         },
         options
       ) do
    start_key = Pack.to_fdb_datakey(adapter_opts, plan.source, param_left)
    end_key = Pack.to_fdb_datakey(adapter_opts, plan.source, param_right)
    start_key = if plan.inclusive_left?, do: start_key, else: :erlfdb_key.strinc(start_key)
    end_key = if plan.inclusive_right?, do: :erlfdb_key.strinc(end_key), else: end_key
    start_key = options[:start_key] || start_key
    %QueryPlan.Between{plan | layer_data: Map.put(layer_data, :range, {start_key, end_key})}
  end

  defp continuation(kvs, options) do
    case options[:limit] do
      nil ->
        %Continuation{more?: false}

      limit ->
        if length(kvs) >= limit do
          {fdb_key, _} = List.last(kvs)
          %Continuation{more?: true, start_key: :erlfdb_key.strinc(fdb_key)}
        else
          %Continuation{more?: false}
        end
    end
  end
end
