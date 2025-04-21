defmodule EctoFoundationDB.Future do
  @moduledoc """
  Opaque struct that represents an unresolved result set from some FoundationDB
  query operation.

  If you have received a Future from an EctoFDB Repo API, please consult the documentation
  for that API for dealing with the Future. The functions here are intended to be for
  internal use only.
  """
  alias EctoFoundationDB.Layer.Tx
  defstruct [:ref, :tx, :erlfdb_future, :result, :handler, :must_wait?]

  @token :__ectofdbfuture__

  def token(), do: @token

  # Before entering a transactional that uses this Future module, we must know whether or not
  # we are required to wait on the Future upon leaving the transactional (`must_wait?`).
  def before_transactional() do
    %__MODULE__{
      ref: nil,
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

  def new() do
    %__MODULE__{handler: &Function.identity/1, must_wait?: true}
  end

  def ref(%__MODULE__{ref: ref}), do: ref

  @doc """
  Creates a Future that will be resolved outside of the transaction in which it was created.

  Used for:

    - watch
    - versionstamp
  """
  def new_deferred(erlfdb_future, handler \\ &Function.identity/1) do
    # The future for a watch and versionstamp is fulfilled outside of a transaction, so there is no tx val
    %__MODULE__{
      tx: :deferred,
      ref: get_ref(erlfdb_future),
      erlfdb_future: erlfdb_future,
      handler: handler,
      must_wait?: false
    }
  end

  def set(fut, tx, erlfdb_future, f \\ &Function.identity/1) do
    ref = get_ref(erlfdb_future)
    %__MODULE__{handler: g} = fut
    %__MODULE__{fut | ref: ref, tx: tx, erlfdb_future: erlfdb_future, handler: &f.(g.(&1))}
  end

  def set_result(fut, result) do
    %__MODULE__{handler: f} = fut

    %__MODULE__{
      fut
      | tx: nil,
        erlfdb_future: nil,
        result: f.(result),
        handler: &Function.identity/1
    }
  end

  def result(fut = %__MODULE__{erlfdb_future: nil, handler: f}) do
    f.(fut.result)
  end

  def result(fut = %__MODULE__{tx: :deferred}) do
    %__MODULE__{erlfdb_future: erlfdb_future, handler: handler} = fut
    res = :erlfdb.wait(erlfdb_future)
    handler.(res)
  end

  def result(fut) do
    %__MODULE__{tx: tx, erlfdb_future: erlfdb_future, handler: handler} = fut

    # Since we only have a single future, we can use :erlfdb.wait/1 as long as it's
    # not a :fold_future. Doing so is good for bookkeeping (fdb_api_counting_test)
    res =
      case elem(erlfdb_future, 0) do
        :fold_future ->
          [res] = :erlfdb.wait_for_all_interleaving(tx, [erlfdb_future])
          res

        _ ->
          :erlfdb.wait(erlfdb_future)
      end

    handler.(res)
  end

  def await_all(futs) do
    futs
    |> await_stream()
    |> Enum.to_list()
  end

  # Future: If there is a wrapping transaction with an `async_*` qualifier, the wait happens here
  def await_stream(futs) do
    futs = Enum.to_list(futs)

    # important to maintain order of the input futures
    reffed_futures =
      for %__MODULE__{ref: ref, erlfdb_future: erlfdb_future} <- futs,
          not is_nil(erlfdb_future),
          do: {ref, erlfdb_future}

    results =
      if length(reffed_futures) > 0 do
        [%__MODULE__{tx: tx} | _] = futs
        {refs, erlfdb_futures} = Enum.unzip(reffed_futures)
        Enum.zip(refs, :erlfdb.wait_for_all_interleaving(tx, erlfdb_futures)) |> Enum.into(%{})
      else
        %{}
      end

    Stream.map(
      futs,
      fn fut = %__MODULE__{ref: ref, result: result, handler: f} ->
        case Map.get(results, ref, nil) do
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

  defp get_ref({:erlfdb_future, ref, _}), do: ref
  defp get_ref({:fold_future, _, {:erlfdb_future, ref, _}}), do: ref
  defp get_ref(_), do: :erlang.error(:badarg)
end
