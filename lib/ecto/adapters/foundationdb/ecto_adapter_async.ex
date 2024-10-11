defmodule Ecto.Adapters.FoundationDB.EctoAdapterAsync do
  @moduledoc false
  alias EctoFoundationDB.Future

  def async_query(_module, repo, fun) do
    # Executes the repo function (e.g. get, get_by, all, etc). Caller must ensure
    # that the proper `:returning` option is used to adhere to the async/await
    # contract.
    _res = fun.()

    case Process.delete(Future.token()) do
      nil ->
        raise "Pipelining failure"

      future ->
        schema = Future.schema(future)

        Future.apply(future, fn {all_or_one, result} ->
          handle_all_or_one(repo, schema, all_or_one, result)
        end)
    end
  after
    Process.delete(Future.token())
  end

  defp handle_all_or_one(repo, schema, all_or_one, result) do
    if is_nil(result), do: raise("Pipelining failure")

    # Abuse a :noop option here to signal to the backend that we don't
    # actually want to run a query. Instead, we just want the result to
    # be transformed by Ecto's internal logic.
    case all_or_one do
      :all ->
        repo.all(schema, noop: result)

      :one ->
        repo.one(schema, noop: result)
    end
  end
end
