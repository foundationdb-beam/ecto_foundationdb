defmodule Ecto.Adapters.FoundationDB.EctoAdapterAsync do
  @moduledoc false
  alias Ecto.Adapters.FoundationDB
  alias EctoFoundationDB.Exception.Unsupported
  alias EctoFoundationDB.Future
  alias EctoFoundationDB.Layer.Fields
  alias EctoFoundationDB.Layer.Tx
  alias EctoFoundationDB.Versionstamp
  import Ecto.Query

  def async_insert_all!(_module, repo, schema, list, opts) do
    {tx?, tenant} = Tx.in_tenant_tx?()

    if not tx?,
      do: raise(Unsupported, "`Repo.async_insert_all!` must be called within a transaction")

    pk_field = Fields.get_pk_field!(schema)

    tx = Tx.get()

    forced_no_conflict? = [] == Keyword.get(opts, :conflict_target)

    list =
      for x <- list do
        if not is_struct(x, schema) do
          raise Unsupported, """
          `Repo.async_insert_all!` must be called with a list of Ecto.Schema structs
          """
        end

        pk = Map.get(x, pk_field)

        x =
          if is_nil(pk) and
               schema.__schema__(:type, pk_field) == Versionstamp do
            Map.put(x, pk_field, Versionstamp.next(tx))
          else
            x
          end

        pk = Map.get(x, pk_field)

        if not forced_no_conflict? and not Versionstamp.incomplete?(pk) do
          raise Unsupported, """
          `Repo.async_insert_all!` is designed to be called with either

          1. A list of Ecto.Schema structs with incomplete Versionstamp in the `:id` field
          2. The option `conflict_target: []`. (Make sure you understand the implications of this option)
          """
        end

        x
      end

    # The insert_all function does not return the structs, so instead we make sure individual calls to `insert!` are
    # non-blocking by enforcing Versionstamps or conflict_target == []
    result = for x <- list, do: repo.insert!(x, opts)

    vs_future = Versionstamp.get(tx)

    Future.apply(vs_future, fn vs ->
      Enum.map(result, &resolve_versionstamp(tenant, &1, vs, pk_field))
    end)
  end

  def async_query(_module, repo, fun) do
    # Executes the repo function (e.g. get, get_by, all, etc). Caller must ensure
    # that the proper `:returning` option is used to adhere to the async/await
    # contract.
    _res = fun.()

    case Process.delete(Future.token()) do
      nil ->
        raise "Pipelining failure"

      {{source, schema}, future} ->
        Future.apply(future, fn {return_handler, result} ->
          invoke_return_handler(repo, source, schema, return_handler, result)
        end)
    end
  after
    Process.delete(Future.token())
  end

  defp invoke_return_handler(repo, source, schema, return_handler, result) do
    if is_nil(result), do: raise("Pipelining failure")

    queryable = if is_nil(schema), do: source, else: schema

    # Abuse a :noop option here to signal to the backend that we don't
    # actually want to run a query. Instead, we just want the result to
    # be transformed by Ecto's internal logic.
    case return_handler do
      :all ->
        repo.all(queryable, noop: result)

      :one ->
        repo.one(queryable, noop: result)

      :all_from_source ->
        {select_fields, data_result} = result
        query = from(_ in source, select: ^select_fields)
        repo.all(query, noop: data_result)
    end
  end

  defp resolve_versionstamp(tenant, x, vs, pk_field) do
    pk = Map.get(x, pk_field)

    x =
      if Versionstamp.incomplete?(pk) do
        pk = Versionstamp.resolve(Map.get(x, pk_field), vs)
        Map.put(x, pk_field, pk)
      else
        x
      end

    FoundationDB.usetenant(x, tenant)
  end
end
