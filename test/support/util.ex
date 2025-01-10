defmodule EctoFoundationDB.Test.Util do
  @moduledoc false
  def get_random_bytes(size) do
    case :persistent_term.get({__MODULE__, :random_bytes, size}, nil) do
      nil ->
        bytes = weak_rand_bytes(size)
        :persistent_term.put({__MODULE__, :random_bytes, size}, bytes)
        bytes

      bytes ->
        bytes
    end
  end

  defp weak_rand_bytes(n) do
    s = :rand.seed_s(:exsss, {0, 0, 0})

    {nums, _} =
      Enum.reduce(1..n, {[], s}, fn _, {acc, s} ->
        {r, s} = :rand.uniform_s(256, s)
        {[r - 1 | acc], s}
      end)

    nums
    |> Enum.reverse()
    |> :erlang.list_to_binary()
  end
end
