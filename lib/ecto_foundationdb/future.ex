defmodule EctoFoundationDB.Future do
  @moduledoc """
  Opaque struct that represents an unresolved result set from some FoundationDB
  query operation.

  If you have received a Future from an EctoFDB Repo API, please consult the documentation
  for that API for dealing with the Future. The functions here are intended to be for
  internal use only.
  """
  alias EctoFoundationDB.Layer.Tx
  defstruct [:ref, :schema, :tx, :erlfdb_future, :result, :handler, :must_wait?]

  @token :__ectofdbfuture__

  def token(), do: @token

  # Before entering a transactional that uses this Future module, we must know whether or not
  # we are required to wait on the Future upon leaving the transactional (`must_wait?`).
  def before_transactional(schema) do
    %__MODULE__{
      ref: nil,
      schema: schema,
      handler: &Function.identity/1,
      must_wait?: not Tx.in_tx?()
    }
  end

  # Upon leaving a transactional that uses this Future module, `leaving_transactional` must
  # be called so that any Futures have a change to wait on their results if necessary.
  def leaving_transactional(fut = %__MODULE__{must_wait?: true}) do
    %__MODULE__{
      fut
      | tx: nil,
        erlfdb_future: nil,
        result: result(fut),
        handler: &Function.identity/1
    }
  end

  def leaving_transactional(fut = %__MODULE__{must_wait?: false}) do
    fut
  end

  def schema(%__MODULE__{schema: schema}), do: schema

  def new(schema) do
    %__MODULE__{schema: schema, handler: &Function.identity/1, must_wait?: true}
  end

  def ref(%__MODULE__{ref: ref}), do: ref

  def new_watch(schema, erlfdb_future, handler \\ &Function.identity/1) do
    # The future for a watch is fulfilled outside of a transaction, so there is no tx val
    %__MODULE__{
      schema: schema,
      tx: :watch,
      erlfdb_future: erlfdb_future,
      handler: handler,
      must_wait?: false
    }
  end

  def set(fut, tx, erlfdb_future, f \\ &Function.identity/1) do
    ref =
      case erlfdb_future do
        {:erlfdb_future, ref, _} -> ref
        {:fold_future, _, {:erlfdb_future, ref, _}} -> ref
        _ -> :erlang.error(:badarg)
      end

    %__MODULE__{handler: g} = fut
    %__MODULE__{fut | ref: ref, tx: tx, erlfdb_future: erlfdb_future, handler: &f.(g.(&1))}
  end

  def result(fut = %__MODULE__{erlfdb_future: nil, handler: f}) do
    f.(fut.result)
  end

  def result(fut = %__MODULE__{tx: :watch}) do
    %__MODULE__{erlfdb_future: erlfdb_future, handler: handler} = fut
    res = :erlfdb.wait(erlfdb_future)
    handler.(res)
  end

  def result(fut) do
    %__MODULE__{tx: tx, erlfdb_future: erlfdb_future, handler: handler} = fut
    [res] = :erlfdb.wait_for_all_interleaving(tx, [erlfdb_future])
    handler.(res)
  end

  def await_ready(futs) do
    await_ready(futs, [])
  end

  def await_all(futs) do
    futs
    |> await_stream()
    |> Enum.to_list()
  end

  # Future: If there is a wrapping transaction with an `async_*` qualifier, the wait happens here
  def await_stream(futs) do
    tx_refs = get_tx_and_refs(futs)

    results =
      if length(tx_refs) > 0 do
        {[tx | _], refs} = Enum.unzip(tx_refs)
        Enum.zip(refs, :erlfdb.wait_for_all_interleaving(tx, refs)) |> Enum.into(%{})
      else
        %{}
      end

    Stream.map(
      futs,
      fn fut = %__MODULE__{erlfdb_future: erlfdb_future, result: result, handler: f} ->
        case Map.get(results, erlfdb_future, nil) do
          nil ->
            %__MODULE__{
              fut
              | tx: nil,
                erlfdb_future: nil,
                result: f.(result),
                handler: &Function.identity/1
            }

          new_result ->
            %__MODULE__{
              fut
              | tx: nil,
                erlfdb_future: nil,
                result: f.(new_result),
                handler: &Function.identity/1
            }
        end
      end
    )
  end

  def apply(fut = %__MODULE__{erlfdb_future: nil}, f) do
    %__MODULE__{handler: g, result: result} = fut
    %__MODULE__{result: f.(g.(result)), handler: &Function.identity/1}
  end

  def apply(fut, f) do
    %__MODULE__{handler: g} = fut
    %__MODULE__{fut | handler: &f.(g.(&1))}
  end

  def find_ready(futs, ready_ref) do
    # Must only be called if you've received a {ready_ref, :ready}
    # message in your mailbox.

    find_ready(futs, ready_ref, [])
  end

  defp find_ready([], _ready_ref, acc), do: {nil, Enum.reverse(acc)}

  defp find_ready([h | t], ready_ref, acc) do
    %__MODULE__{erlfdb_future: erlfdb_future} = h

    if match_ref?(erlfdb_future, ready_ref) do
      {h, Enum.reverse(acc) ++ t}
    else
      find_ready(t, ready_ref, [h | acc])
    end
  end

  defp match_ref?({:erlfdb_future, ref1, _}, ref2) when ref1 === ref2, do: true
  defp match_ref?({:fold_future, _, erlfdb_future}, ref2), do: match_ref?(erlfdb_future, ref2)
  defp match_ref?(_, _), do: false

  defp await_ready(futs, resend_q) do
    # Inspired by :erlfdb.wait_for_any
    receive do
      {ready_ref, :ready} ->
        case find_ready(futs, ready_ref) do
          {nil, futs} ->
            await_ready(futs, [ready_ref | resend_q])

          {ready, remaining} ->
            for m <- resend_q, do: send(self(), m)
            {ready, remaining}
        end
    end
  end

  defp get_tx_and_refs(futs) do
    # important to maintain order of the input futures
    for %__MODULE__{tx: tx, erlfdb_future: erlfdb_future} <- futs,
        not is_nil(erlfdb_future),
        do: {tx, erlfdb_future}
  end
end
