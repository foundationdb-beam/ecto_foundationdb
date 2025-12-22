defmodule Ecto.Adapters.FoundationDB.EctoAdapterAssigns do
  @moduledoc false
  alias EctoFoundationDB.Future
  alias EctoFoundationDB.Indexer.SchemaMetadata
  alias EctoFoundationDB.Layer.Tx

  alias Ecto.Adapters.FoundationDB

  def assign_ready(_module, repo, futures, ready_refs, options) when is_list(ready_refs) do
    {labeled_res, untouched_futures} =
      Tx.transactional(options[:prefix], fn _tx ->
        {labeled_futures_rev, futures} = filter_ready(repo, futures, ready_refs, options)

        labeled_futures = Enum.reverse(labeled_futures_rev)
        {labels, assign_futures} = Enum.unzip(labeled_futures)
        res = repo.await(assign_futures)
        labeled_res = Enum.zip(labels, res)
        {labeled_res, futures}
      end)

    empty_futures = if is_list(futures), do: [], else: %{}

    {assigns, new_futures} =
      Enum.reduce(labeled_res, {[], empty_futures}, fn
        {label, {new_assigns, new_future_or_nil}}, {assigns, new_futures} ->
          {assigns ++ new_assigns, append_new_future(new_futures, label, new_future_or_nil)}
      end)

    {assigns, new_futures, untouched_futures}
  end

  defp filter_ready(repo, futures, ready_refs, options) do
    Enum.reduce(ready_refs, {[], futures}, fn ready_ref, {acc, futures} ->
      case async_assign_ready(__MODULE__, repo, futures, ready_ref, options) do
        {_label, nil, futures} ->
          {acc, futures}

        {label, assign_future, futures} ->
          {[{label, assign_future} | acc], futures}
      end
    end)
  end

  defp append_new_future(futures, _label, nil), do: futures
  defp append_new_future(futures, _label, future) when is_list(futures), do: [future | futures]

  defp append_new_future(futures, label, future) when is_map(futures) do
    case Map.fetch(futures, label) do
      {:ok, f} ->
        Future.cancel(f)

      _ ->
        :ok
    end

    Map.put(futures, label, future)
  end

  def async_assign_ready(_module, repo, futures, ready_ref, options)
      when is_reference(ready_ref) do
    case Future.find_ready(futures, ready_ref) do
      {label, nil, futures} ->
        {label, nil, futures}

      {label, found_future, futures} ->
        {schema, kind, watch_options, new_watch_fn} = Future.result(found_future)

        if is_nil(label) and not Keyword.has_key?(watch_options, :label) do
          raise """
          To use Repo.assign_ready/3, you must either

          1. Create the original watch with a label

          Examples:

              Repo.watch(struct, label: :mykey)
              SchemaMetadata.watch_collection(MySchema, label: :mykey)

          2. Call Repo.assign_ready/3 with a labeled map of futures

          Example:

              Repo.assign_ready(%{mykey: future}, [ready_ref], watch?: true, prefix: tenant)
          """
        end

        case kind do
          {:pk, pk} ->
            async_get(repo, label, futures, schema, pk, watch_options, options, new_watch_fn)

          {SchemaMetadata, action}
          when action in [:inserts, :deletes, :collection, :updates, :changes] ->
            async_all(repo, label, futures, schema, watch_options, options, new_watch_fn)
        end
    end
  end

  defp async_get(repo, label, futures, schema, id, watch_options, options, new_watch_fn) do
    label = label || watch_options[:label]

    tenant = options[:prefix]

    Tx.transactional(options[:prefix], fn _tx ->
      assign_future =
        repo.async_get(schema, id, options)
        |> Future.apply(fn struct_or_nil ->
          struct_or_nil = usetenant(struct_or_nil, tenant)
          new_future = maybe_new_watch(struct_or_nil, watch_options, options, new_watch_fn)

          {[{label, struct_or_nil}], new_future}
        end)

      {label, assign_future, futures}
    end)
  end

  defp async_all(repo, label, futures, schema, watch_options, options, new_watch_fn) do
    label = label || watch_options[:label]
    query = watch_options[:query] || schema
    tenant = options[:prefix]

    Tx.transactional(tenant, fn _tx ->
      assign_future =
        repo.async_all(query, options)
        |> Future.apply(fn result ->
          result = usetenant(result, tenant)
          new_future = maybe_new_watch(result, watch_options, options, new_watch_fn)

          {[{label, result}], new_future}
        end)

      {label, assign_future, futures}
    end)
  end

  defp usetenant(nil, _tenant), do: nil
  defp usetenant(list, tenant) when is_list(list), do: Enum.map(list, &usetenant(&1, tenant))
  defp usetenant(struct, tenant), do: FoundationDB.usetenant(struct, tenant)

  defp maybe_new_watch(result, watch_options, options, new_watch_fn) do
    if Keyword.get(options, :watch?, false) do
      new_watch_fn.(result, watch_options)
    else
      nil
    end
  end
end
