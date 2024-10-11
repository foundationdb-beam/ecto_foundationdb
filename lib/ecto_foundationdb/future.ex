defmodule EctoFoundationDB.Future do
  @moduledoc """
  Opaque struct that represents an unresolved result set from some FoundationDB
  query operation.

  If you have received a Future from an EctoFDB Repo API, please consult the documentation
  for that API for dealing with the Future. The functions here are intended to be for
  internal use only.
  """
  alias EctoFoundationDB.Layer.Tx
  defstruct [:schema, :tx, :ref, :result, :handler, :must_wait?]

  @token :__ectofdbfuture__

  def token(), do: @token

  # Before entering a transactional that uses this Future module, we must know whether or not
  # we are required to wait on the Future upon leaving the transactional (`must_wait?`).
  def before_transactional(schema) do
    %__MODULE__{schema: schema, handler: &Function.identity/1, must_wait?: not Tx.in_tx?()}
  end

  # Upon leaving a transactional that uses this Future module, `leaving_transactional` must
  # be called so that any Futures have a change to wait on their results if necessary.
  def leaving_transactional(fut = %__MODULE__{must_wait?: true}) do
    %__MODULE__{fut | tx: nil, ref: nil, result: result(fut), handler: &Function.identity/1}
  end

  def leaving_transactional(fut = %__MODULE__{must_wait?: false}) do
    fut
  end

  def schema(%__MODULE__{schema: schema}), do: schema

  def new(schema) do
    %__MODULE__{schema: schema, handler: &Function.identity/1, must_wait?: true}
  end

  def new_watch(schema, future_ref, handler \\ &Function.identity/1) do
    # The future for a watch is fulfilled outside of a transaction, so there is no tx val
    %__MODULE__{
      schema: schema,
      tx: :watch,
      ref: future_ref,
      handler: handler,
      must_wait?: false
    }
  end

  def set(fut, tx, future_ref, f \\ &Function.identity/1) do
    %__MODULE__{handler: g} = fut
    %__MODULE__{fut | tx: tx, ref: future_ref, handler: &f.(g.(&1))}
  end

  def result(fut = %__MODULE__{ref: nil, handler: f}) do
    f.(fut.result)
  end

  def result(fut = %__MODULE__{tx: :watch}) do
    %__MODULE__{ref: ref, handler: handler} = fut
    res = :erlfdb.wait(ref)
    handler.(res)
  end

  def result(fut) do
    %__MODULE__{tx: tx, ref: ref, handler: handler} = fut
    [res] = :erlfdb.wait_for_all_interleaving(tx, [ref])
    handler.(res)
  end

  # Future: If there is a wrapping transaction with an `async_*` qualifier, the wait happens here
  def await_all(futs) do
    # important to maintain order of the input futures
    tx_refs = for %__MODULE__{tx: tx, ref: ref} <- futs, not is_nil(ref), do: {tx, ref}

    results =
      if length(tx_refs) > 0 do
        {[tx | _], refs} = Enum.unzip(tx_refs)
        Enum.zip(refs, :erlfdb.wait_for_all_interleaving(tx, refs)) |> Enum.into(%{})
      else
        %{}
      end

    Enum.map(
      futs,
      fn fut = %__MODULE__{ref: ref, result: result, handler: f} ->
        case Map.get(results, ref, nil) do
          nil ->
            %__MODULE__{
              fut
              | tx: nil,
                ref: nil,
                result: f.(result),
                handler: &Function.identity/1
            }

          new_result ->
            %__MODULE__{
              fut
              | tx: nil,
                ref: nil,
                result: f.(new_result),
                handler: &Function.identity/1
            }
        end
      end
    )
  end

  def apply(fut = %__MODULE__{ref: nil}, f) do
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
    %__MODULE__{ref: ref} = h

    if match?({:erlfdb_future, ^ready_ref, _}, ref) do
      {%__MODULE__{h | ref: nil}, Enum.reverse(acc) ++ t}
    else
      find_ready(t, ready_ref, [h | acc])
    end
  end
end
