defmodule EctoFoundationDB.Layer.Query do
  @moduledoc false
  alias EctoFoundationDB.Exception.Unsupported
  alias EctoFoundationDB.Future
  alias EctoFoundationDB.Indexer
  alias EctoFoundationDB.Layer.Fields
  alias EctoFoundationDB.Layer.IndexInventory
  alias EctoFoundationDB.Layer.Pack
  alias EctoFoundationDB.Layer.Tx
  alias EctoFoundationDB.QueryPlan
  alias EctoFoundationDB.Schema

  defmodule Continuation do
    @moduledoc false
    defstruct more?: false, start_key: nil
  end

  @doc """
  Executes a query for retrieving data.
  """
  def all(tenant, adapter_meta, plan, options \\ []) do
    # All data retrieval comes through here. We use the Future module to
    # ensure that if the whole thing is executed inside a wrapping transaction,
    # the result is not awaited upon until requested. But if we're creating a new
    # transaction, the wait happens before the transaction ends.

    future = Future.before_transactional(plan.schema)

    {plan, future} =
      IndexInventory.transactional(tenant, adapter_meta, plan.source, fn tx,
                                                                         idxs,
                                                                         _partial_idxs ->
        plan = make_range(idxs, plan, options)
        future = tx_get_range(tx, plan, future, options)

        # Future: If there is no wrapping transaction, the wait happens here
        {plan, Future.leaving_transactional(future)}
      end)

    Future.apply(future, fn kvs ->
      continuation = continuation(kvs, options)

      objs =
        kvs
        |> unpack_and_filter(plan)
        |> Stream.map(fn {_k, v} -> v end)

      {objs, continuation}
    end)
  end

  @doc """
  Executes a query for updating data.
  """
  def update(tenant, adapter_meta, plan) do
    IndexInventory.transactional(tenant, adapter_meta, plan.source, fn tx, idxs, partial_idxs ->
      plan = make_range(idxs, plan, [])
      tx_update_range(tx, plan, idxs, partial_idxs)
    end)
  end

  @doc """
  Executes a query for deleting data.
  """
  def delete(
        tenant,
        adapter_meta,
        plan = %QueryPlan{tenant: tenant, constraints: [%QueryPlan.None{}]}
      ) do
    # Special case, very efficient
    Tx.transactional(tenant, &Tx.clear_all(tenant, &1, adapter_meta, plan.source))
  end

  def delete(tenant, adapter_meta, plan) do
    IndexInventory.transactional(tenant, adapter_meta, plan.source, fn tx, idxs, partial_idxs ->
      plan = make_range(idxs, plan, [])
      tx_delete_range(tx, plan, idxs, partial_idxs)
    end)
  end

  defp make_range(
         _idxs,
         plan = %QueryPlan{constraints: [%{is_pk?: true}]},
         options
       ) do
    make_datakey_range(plan, options)
  end

  defp make_range(
         idxs,
         plan = %QueryPlan{constraints: constraints, layer_data: layer_data},
         options
       ) do
    case IndexInventory.select_index(idxs, constraints) do
      nil ->
        raise Unsupported,
              """
              FoundationDB Adapter supports either a where clause that constrains on the primary key
              or a where clause that constrains on a set of fields that is associated with an index.
              """

      idx ->
        constraints = IndexInventory.arrange_constraints(constraints, idx)
        plan = %QueryPlan{plan | constraints: constraints}
        range = Indexer.range(idx, plan, options)

        layer_data =
          layer_data
          |> Map.put(:idx, idx)
          |> Map.put(:range, range)

        %QueryPlan{plan | layer_data: layer_data}
    end
  end

  defp tx_get_range(tx, %{layer_data: %{range: {fdb_key, nil}}}, future, _options) do
    future_ref = :erlfdb.get(tx, fdb_key)

    Future.set(future, tx, future_ref, fn res ->
      if res == :not_found do
        []
      else
        [{fdb_key, res}]
      end
    end)
  end

  defp tx_get_range(tx, %{layer_data: %{range: {start_key, end_key}}}, future, options) do
    get_options = Keyword.take(options, [:limit]) |> Keyword.put(:wait, false)
    future_ref = :erlfdb.get_range(tx, start_key, end_key, get_options)
    Future.set(future, tx, future_ref)
  end

  defp tx_get_range(tx, %{layer_data: %{range: {start_key, end_key, mapper}}}, future, options) do
    get_options = Keyword.take(options, [:limit]) |> Keyword.put(:wait, false)
    future_ref = :erlfdb.get_mapped_range(tx, start_key, end_key, mapper, get_options)
    Future.set(future, tx, future_ref)
  end

  defp tx_update_range(tx, plan = %QueryPlan{updates: updates}, idxs, partial_idxs) do
    pk_field = Fields.get_pk_field!(plan.schema)
    write_primary = Schema.get_option(plan.context, :write_primary)

    tx
    |> tx_get_range(plan, Future.new(plan.schema), [])
    |> Future.result()
    |> unpack_and_filter(plan)
    |> Stream.map(
      &Tx.update_data_object(
        plan.tenant,
        tx,
        plan.schema,
        pk_field,
        &1,
        updates,
        {idxs, partial_idxs},
        write_primary
      )
    )
    |> Enum.to_list()
    |> length()
  end

  defp tx_delete_range(tx, plan, idxs, partial_idxs) do
    tx
    |> tx_get_range(plan, Future.new(plan.schema), [])
    |> Future.result()
    |> unpack_and_filter(plan)
    |> Stream.map(&Tx.delete_data_object(plan.tenant, tx, plan.schema, &1, {idxs, partial_idxs}))
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
         plan = %QueryPlan{constraints: [%QueryPlan.None{}], layer_data: layer_data},
         options
       ) do
    {start_key, end_key} = Pack.primary_range(plan.tenant, plan.source)
    start_key = options[:start_key] || start_key
    %QueryPlan{plan | layer_data: Map.put(layer_data, :range, {start_key, end_key})}
  end

  defp make_datakey_range(
         plan = %QueryPlan{
           tenant: tenant,
           constraints: [%QueryPlan.Equal{param: param}],
           layer_data: layer_data
         },
         _options
       ) do
    fdb_key = Pack.primary_pack(tenant, plan.source, param)
    %QueryPlan{plan | layer_data: Map.put(layer_data, :range, {fdb_key, nil})}
  end

  defp make_datakey_range(
         plan = %QueryPlan{
           tenant: tenant,
           constraints: [
             between =
               %QueryPlan.Between{
                 param_left: param_left,
                 param_right: param_right
               }
           ],
           layer_data: layer_data
         },
         options
       )
       when is_binary(param_left) and is_binary(param_right) do
    param_left = if between.inclusive_left?, do: param_left, else: :erlfdb_key.strinc(param_left)

    param_right =
      if between.inclusive_right?, do: :erlfdb_key.strinc(param_right), else: param_right

    start_key = Pack.primary_pack(tenant, plan.source, param_left)
    end_key = Pack.primary_pack(tenant, plan.source, param_right)
    start_key = options[:start_key] || start_key
    %QueryPlan{plan | layer_data: Map.put(layer_data, :range, {start_key, end_key})}
  end

  defp make_datakey_range(_plan, _options) do
    raise Unsupported, "Between query must have binary parameters"
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
