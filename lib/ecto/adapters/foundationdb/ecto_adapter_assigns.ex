defmodule Ecto.Adapters.FoundationDB.EctoAdapterAssigns do
  @moduledoc false
  alias EctoFoundationDB.Future
  alias EctoFoundationDB.Indexer.SchemaMetadata
  alias EctoFoundationDB.Layer.Tx

  def assign_ready(_module, repo, futures, ready_refs, options) when is_list(ready_refs) do
    Tx.transactional(options[:prefix], fn _tx ->
      {assign_futures_rev, futures} = filter_ready(repo, futures, ready_refs, options)

      res = repo.await(Enum.reverse(assign_futures_rev))

      Enum.reduce(res, {[], futures}, fn
        {new_assigns, new_future_or_nil}, {assigns, futures} ->
          {assigns ++ new_assigns, append_new_future(futures, new_future_or_nil)}
      end)
    end)
  end

  defp filter_ready(repo, futures, ready_refs, options) do
    Enum.reduce(ready_refs, {[], futures}, fn ready_ref, {acc, futures} ->
      case async_assign_ready(__MODULE__, repo, futures, ready_ref, options) do
        {nil, futures} ->
          {acc, futures}

        {assign_future, futures} ->
          {[assign_future | acc], futures}
      end
    end)
  end

  defp append_new_future(futures, nil), do: futures
  defp append_new_future(futures, future), do: [future | futures]

  def async_assign_ready(_module, repo, futures, ready_ref, options)
      when is_reference(ready_ref) do
    case Future.find_ready(futures, ready_ref) do
      {nil, futures} ->
        {nil, futures}

      {future, futures} ->
        {schema, query, watch_options, new_watch_fn} = Future.result(future)

        if not Keyword.has_key?(watch_options, :label) do
          raise """
          To use Repo.assign_ready/3, you must have previously created a watch with a label

          Examples:

              Repo.watch(struct, label: :mykey)
              SchemaMetadata.watch_collection(MySchema, label: :mykey)
          """
        end

        case query do
          {:pk, pk} ->
            async_get(repo, futures, schema, pk, watch_options, options, new_watch_fn)

          {SchemaMetadata, name}
          when name in [:inserts, :deletes, :collection, :updates, :changes] ->
            async_all(repo, futures, schema, watch_options, options, new_watch_fn)
        end
    end
  end

  defp async_get(repo, futures, schema, id, watch_options, options, new_watch_fn) do
    label = watch_options[:label]

    Tx.transactional(options[:prefix], fn _tx ->
      assign_future =
        repo.async_get(schema, id, options)
        |> Future.apply(fn struct_or_nil ->
          new_future = maybe_new_watch(struct_or_nil, watch_options, options, new_watch_fn)

          {[{label, struct_or_nil}], new_future}
        end)

      {assign_future, futures}
    end)
  end

  defp async_all(repo, futures, schema, watch_options, options, new_watch_fn) do
    label = watch_options[:label]

    Tx.transactional(options[:prefix], fn _tx ->
      assign_future =
        repo.async_all(schema, options)
        |> Future.apply(fn result ->
          new_future = maybe_new_watch(result, watch_options, options, new_watch_fn)

          {[{label, result}], new_future}
        end)

      {assign_future, futures}
    end)
  end

  defp maybe_new_watch(result, watch_options, options, new_watch_fn) do
    if Keyword.get(options, :watch?, false) do
      new_watch_fn.(result, watch_options)
    else
      nil
    end
  end
end
