defmodule EctoFoundationDB.Layer.Query do
  @moduledoc false
  alias EctoFoundationDB.Exception.Unsupported
  alias EctoFoundationDB.Future
  alias EctoFoundationDB.Indexer
  alias EctoFoundationDB.Layer.DecodedKV
  alias EctoFoundationDB.Layer.Fields
  alias EctoFoundationDB.Layer.Metadata
  alias EctoFoundationDB.Layer.Pack
  alias EctoFoundationDB.Layer.PrimaryKVCodec
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
  def all(tenant, adapter_meta, plan, options) do
    # All data retrieval comes through here. We use the Future module to
    # ensure that if the whole thing is executed inside a wrapping transaction,
    # the result is not awaited upon until requested. But if we're creating a new
    # transaction, the wait happens before the transaction ends.

    future = Future.before_transactional()

    {plan, future} =
      Metadata.transactional(tenant, adapter_meta, plan.source, fn tx, metadata ->
        plan = make_range(metadata, plan, options)
        future = tx_get_range(tx, plan, future, options)

        # Future: If there is no wrapping transaction, the wait happens here
        {plan, Future.leaving_transactional(future)}
      end)

    Future.apply(future, fn kvs ->
      continuation = continuation(kvs, options)

      objs =
        kvs
        |> unpack_and_filter(plan)
        |> Stream.map(fn %DecodedKV{data_object: v} -> v end)

      {objs, continuation}
    end)
  end

  @doc """
  Executes a query for updating data.
  """
  def update(tenant, adapter_meta, plan, options) do
    Metadata.transactional(tenant, adapter_meta, plan.source, fn tx, metadata ->
      plan = make_range(metadata, plan, [])
      tx_update_range(tx, plan, metadata, options)
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
    Metadata.transactional(tenant, adapter_meta, plan.source, fn tx, metadata ->
      plan = make_range(metadata, plan, [])
      tx_delete_range(tx, plan, metadata)
    end)
  end

  defp make_range(
         _metadata,
         plan = %QueryPlan{constraints: [%{is_pk?: true}]},
         options
       ) do
    make_datakey_range(plan, options)
  end

  defp make_range(
         metadata,
         plan = %QueryPlan{constraints: constraints, layer_data: layer_data},
         options
       ) do
    case Metadata.select_index(metadata, constraints) do
      nil ->
        raise Unsupported,
              """
              FoundationDB Adapter supports either a where clause that constrains on the primary key
              or a where clause that constrains on a set of fields that is associated with an index.
              """

      idx ->
        constraints = Metadata.arrange_constraints(constraints, idx)
        plan = %QueryPlan{plan | constraints: constraints}
        range = Indexer.range(idx, plan, options)

        layer_data =
          layer_data
          |> Map.put(:idx, idx)
          |> Map.put(:range, range)

        %QueryPlan{plan | layer_data: layer_data}
    end
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

  defp tx_update_range(
         tx,
         plan = %QueryPlan{updates: updates},
         metadata,
         options
       ) do
    pk_field = Fields.get_pk_field!(plan.schema)
    write_primary = Schema.get_option(plan.context, :write_primary)

    tx
    |> tx_get_range(plan, Future.new(), [])
    |> Future.result()
    |> unpack_and_filter(plan)
    |> Stream.map(
      &Tx.update_data_object(
        plan.tenant,
        tx,
        plan.schema,
        pk_field,
        {&1, updates},
        metadata,
        write_primary,
        options
      )
    )
    |> Enum.to_list()
    |> length()
  end

  defp tx_delete_range(tx, plan, metadata) do
    tx
    |> tx_get_range(plan, Future.new(), [])
    |> Future.result()
    |> unpack_and_filter(plan)
    |> Stream.map(&Tx.delete_data_object(plan.tenant, tx, plan.schema, &1, metadata))
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

  defp unpack_and_filter(kvs, plan) do
    PrimaryKVCodec.stream_decode(kvs, plan.tenant)
  end

  # Selects all data from source
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
    kv_codec = Pack.primary_codec(tenant, plan.source, param)
    %QueryPlan{plan | layer_data: Map.put(layer_data, :range, PrimaryKVCodec.range(kv_codec))}
  end

  defp make_datakey_range(
         plan = %QueryPlan{constraints: [between = %QueryPlan.Between{}]},
         options
       ) do
    %QueryPlan{tenant: tenant, layer_data: layer_data} = plan

    %QueryPlan.Between{
      param_left: param_left,
      inclusive_left?: inclusive_left?,
      param_right: param_right,
      inclusive_right?: inclusive_right?
    } = between

    {left_range_start, left_range_end} =
      if is_nil(param_left) do
        Pack.primary_range(tenant, plan.source)
      else
        codec_left = Pack.primary_codec(tenant, plan.source, param_left)
        PrimaryKVCodec.range(codec_left)
      end

    {right_range_start, right_range_end} =
      if is_nil(param_right) do
        Pack.primary_range(tenant, plan.source)
      else
        codec_right = Pack.primary_codec(tenant, plan.source, param_right)
        PrimaryKVCodec.range(codec_right)
      end

    start_key = if inclusive_left?, do: left_range_start, else: left_range_end
    end_key = if inclusive_right?, do: right_range_end, else: right_range_start

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
