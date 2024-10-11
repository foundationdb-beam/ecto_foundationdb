defmodule Ecto.Adapters.FoundationDB.EctoAdapterAssigns do
  @moduledoc false
  alias EctoFoundationDB.Future
  alias EctoFoundationDB.Layer.Tx

  def assign_ready(_module, repo, futures, ready_refs, options) when is_list(ready_refs) do
    Tx.transactional(options[:prefix], fn _tx ->
      {assign_futures_rev, futures} =
        Enum.reduce(ready_refs, {[], futures}, fn ready_ref, {acc, futures} ->
          {assign_future, futures} = repo.async_assign_ready(futures, ready_ref, options)
          {[assign_future | acc], futures}
        end)

      res = repo.await(Enum.reverse(assign_futures_rev))

      Enum.reduce(res, {[], futures}, fn
        {new_assigns, new_future_or_nil}, {assigns, futures} ->
          {assigns ++ new_assigns, append_new_future(futures, new_future_or_nil)}
      end)
    end)
  end

  defp append_new_future(futures, nil), do: futures
  defp append_new_future(futures, future), do: [future | futures]

  def async_assign_ready(_module, repo, futures, ready_ref, options)
      when is_reference(ready_ref) do
    case Future.find_ready(futures, ready_ref) do
      {nil, futures} ->
        {[], futures}

      {future, futures} ->
        {schema, id, watch_options} = Future.result(future)

        if not Keyword.has_key?(watch_options, :label) do
          raise "To use Repo.assign_ready/3, you must have previously called Repo.watch(struct, label: mykey)."
        end

        async_get(repo, futures, schema, id, watch_options, options)
    end
  end

  defp async_get(repo, futures, schema, id, watch_options, options) do
    label = watch_options[:label]

    Tx.transactional(options[:prefix], fn _tx ->
      assign_future =
        repo.async_get(schema, id, options)
        |> Future.apply(fn struct_or_nil ->
          new_future = maybe_new_watch(repo, struct_or_nil, watch_options, options)

          {[{label, struct_or_nil}], new_future}
        end)

      {assign_future, futures}
    end)
  end

  defp maybe_new_watch(repo, struct_or_nil, watch_options, options) do
    if not is_nil(struct_or_nil) and Keyword.get(options, :watch?, false) do
      repo.watch(struct_or_nil, Keyword.merge(options, watch_options))
    else
      nil
    end
  end
end
