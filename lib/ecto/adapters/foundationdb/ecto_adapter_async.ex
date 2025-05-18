defmodule Ecto.Adapters.FoundationDB.EctoAdapterAsync do
  @moduledoc false
  alias EctoFoundationDB.Future
  import Ecto.Query

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
end
