defmodule EctoFoundationDB.Future do
  @moduledoc """
  Opaque struct that represents an unresolved result set from some FoundationDB
  query operation.

  If you have received a Future from an EctoFDB Repo API, please consult the documentation
  for that API for dealing with the Future. The functions here are intended to be for
  internal use only.
  """
  defstruct [:ref, :type, :promise, :result, :handler]

  @token :__ectofdbfuture__

  def token(), do: @token

  def new(type, promise, f \\ &Function.identity/1)

  def new(:result, result, f) do
    set_result(%__MODULE__{handler: f}, result)
  end

  def new(type = :erlfdb_iterator, promise, f) do
    true = is_tuple(promise)

    %__MODULE__{
      ref: get_ref(type, promise),
      type: type,
      promise: promise,
      handler: f
    }
  end

  def new(type, promise, f)
      when type in [:erlfdb_future, :tx_fold_future, :stream, :erlfdb_iterator] do
    %__MODULE__{
      ref: get_ref(type, promise),
      type: type,
      promise: promise,
      handler: f
    }
  end

  def ref(%__MODULE__{ref: ref}), do: ref

  def get_promise(%__MODULE__{type: type, promise: promise}), do: {type, promise}

  def cancel(future = %__MODULE__{type: :erlfdb_future}) do
    %{promise: erlfdb_future} = future
    :erlfdb.cancel(erlfdb_future, flush: true)
    set_result(%{future | handler: &Function.identity/1}, nil)
  end

  def cancel(future = %__MODULE__{type: :tx_fold_future}) do
    %{promise: {_tx, fold_future}} = future
    :erlfdb.cancel(fold_future, flush: true)
    set_result(%{future | handler: &Function.identity/1}, nil)
  end

  def cancel(future = %__MODULE__{type: :stream}) do
    %{promise: stream} = future

    stream
    |> Stream.take(0)
    |> Stream.run()

    set_result(%{future | handler: &Function.identity/1}, nil)
  end

  def cancel(future = %__MODULE__{type: :erlfdb_iterator}) do
    %{promise: iterator} = future
    :erlfdb_iterator.stop(iterator)
    set_result(%{future | handler: &Function.identity/1}, nil)
  end

  defp set_result(future = %__MODULE__{}, result) do
    %{handler: f} = future

    %{
      future
      | ref: nil,
        type: :result,
        promise: nil,
        result: f.(result),
        handler: &Function.identity/1
    }
  end

  def result(future = %__MODULE__{type: :result}) do
    %{result: result, handler: f} = future
    f.(result)
  end

  def result(future = %__MODULE__{type: :erlfdb_future}) do
    %{promise: erlfdb_future, handler: handler} = future
    res = :erlfdb.wait(erlfdb_future)
    handler.(res)
  end

  def result(future = %__MODULE__{type: :tx_fold_future}) do
    %{promise: {tx, fold_future}, handler: handler} = future
    [res] = :erlfdb.wait_for_all_interleaving(tx, [fold_future])
    handler.(res)
  end

  def result(future = %__MODULE__{type: :stream}) do
    %{promise: stream, handler: handler} = future

    stream
    |> Enum.to_list()
    |> handler.()
  end

  def result(future = %__MODULE__{type: :erlfdb_iterator}) do
    %{promise: iterator, handler: handler} = future

    iterator
    |> FDB.Stream.from_iterator()
    |> Enum.to_list()
    |> handler.()
  end

  def await(future) do
    [future] = await_all([future])
    future
  end

  def await_all(futures) do
    futures
    |> await_stream()
    |> Enum.to_list()
  end

  # Future: If there is a wrapping transaction with an `async_*` qualifier, the wait happens here
  def await_stream(futures) do
    futures = Enum.to_list(futures)

    {tx, reffed_folds, reffed_iterators} = get_pipelineable(futures, nil, %{}, %{})

    results_folds =
      if Enum.empty?(reffed_folds) do
        %{}
      else
        refs = Map.keys(reffed_folds)
        fold_futures = Map.values(reffed_folds)
        Enum.zip(refs, :erlfdb.wait_for_all_interleaving(tx, fold_futures)) |> Enum.into(%{})
      end

    results_iterators =
      if Enum.empty?(reffed_iterators) do
        %{}
      else
        refs = Map.keys(reffed_iterators)
        iterators = Map.values(reffed_iterators)
        iterator_results = :erlfdb_iterator.pipeline(iterators)
        {results, iterators} = Enum.unzip(iterator_results)
        Enum.each(iterators, &:erlfdb_iterator.stop/1)
        Enum.zip(refs, results) |> Enum.into(%{})
      end

    results = Map.merge(results_folds, results_iterators)

    Stream.map(
      futures,
      fn future = %__MODULE__{} ->
        %{ref: ref, type: type, promise: promise, result: result} = future

        case {type, Map.get(results, ref, nil)} do
          {:result, nil} ->
            set_result(future, result)

          {:erlfdb_future, nil} ->
            set_result(future, :erlfdb.wait(promise))

          {:stream, nil} ->
            set_result(future, Enum.to_list(promise))

          {pipelined_type, new_result}
          when pipelined_type in [:tx_fold_future, :erlfdb_iterator] ->
            set_result(future, new_result)
        end
      end
    )
  end

  def then(future = %__MODULE__{type: :result}, f) do
    %{handler: g, result: result} = future
    set_result(future, f.(g.(result)))
  end

  def then(future = %__MODULE__{}, f) do
    %{handler: g} = future
    %{future | handler: &f.(g.(&1))}
  end

  def find_ready(futures, ready_ref) when is_map(futures) do
    {label, found_future, futures} = find_ready(Enum.into(futures, []), ready_ref)
    {label, found_future, Enum.into(futures, %{})}
  end

  def find_ready(futures, ready_ref) when is_list(futures) do
    # Must only be called if you've received a {ready_ref, :ready}
    # message in your mailbox.

    find_ready(futures, ready_ref, [])
  end

  defp find_ready([], _ready_ref, acc), do: {nil, nil, Enum.reverse(acc)}

  defp find_ready([h = %__MODULE__{} | t], ready_ref, acc) do
    %{type: type, promise: promise} = h

    if match_ref?(type, promise, ready_ref) do
      {nil, h, Enum.reverse(acc) ++ t}
    else
      find_ready(t, ready_ref, [h | acc])
    end
  end

  defp find_ready([{label, h = %__MODULE__{}} | t], ready_ref, acc) do
    %{type: type, promise: promise} = h

    if match_ref?(type, promise, ready_ref) do
      {label, h, Enum.reverse(acc) ++ t}
    else
      find_ready(t, ready_ref, [{label, h} | acc])
    end
  end

  # find_ready only useful for watches and versionstamps, so we don't handle the other types
  defp match_ref?(:erlfdb_future, {:erlfdb_future, ref1, _}, ref2) when ref1 === ref2, do: true
  defp match_ref?(_type, _promise, _ref), do: false

  defp get_ref(:erlfdb_future, {:erlfdb_future, ref, _}), do: ref
  defp get_ref(:tx_fold_future, {_tx, {:fold_future, _, {:erlfdb_future, ref, _}}}), do: ref
  defp get_ref(:stream, _), do: make_ref()
  defp get_ref(:erlfdb_iterator, _), do: make_ref()
  defp get_ref(_, _), do: :erlang.error(:badarg)

  defp get_pipelineable([], tx, acc_folds, acc_iterators), do: {tx, acc_folds, acc_iterators}

  defp get_pipelineable(
         [%__MODULE__{type: :tx_fold_future, ref: ref, promise: {tx, fold_future}} | futures],
         _tx,
         acc_folds,
         acc_iterators
       ) do
    get_pipelineable(futures, tx, Map.put(acc_folds, ref, fold_future), acc_iterators)
  end

  defp get_pipelineable(
         [%__MODULE__{type: :erlfdb_iterator, ref: ref, promise: iterator} | futures],
         tx,
         acc_folds,
         acc_iterators
       ) do
    get_pipelineable(futures, tx, acc_folds, Map.put(acc_iterators, ref, iterator))
  end

  defp get_pipelineable([_future | futures], tx, acc_folds, acc_iterators),
    do: get_pipelineable(futures, tx, acc_folds, acc_iterators)
end
