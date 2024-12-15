defmodule EctoFoundationDB.Layer.Splayer do
  alias EctoFoundationDB.Future

  defstruct [:tuple]

  def new(tuple) do
    %__MODULE__{tuple: tuple}
  end

  def pack(splayer, idx) do
    splayer.tuple
    |> splay(idx)
    |> :erlfdb_tuple.pack()
  end

  def range(splayer) do
    tuple = splay(splayer.tuple, nil)
    start_key = :erlfdb_tuple.pack(tuple)
    {start_key, start_key <> <<0xFF>>}
  end

  def async_get(splayer, tx, future) do
    {start_key, end_key} = range(splayer)
    future_ref = :erlfdb.get_range(tx, start_key, end_key, wait: false)

    # Same API contract as :erlfdb.get
    # @todo: handle splayed object
    f = fn
      [] ->
        :not_found

      [{_k, v}] ->
        v
    end

    Future.set(future, tx, future_ref, f)
  end

  # splay({:a, :tuple}, nil) -> {:a, :tuple}
  # splay({:a, :tuple}, 0) -> {:a, :tuple, 0}
  defp splay(tuple, nil), do: tuple

  defp splay(tuple, idx) do
    (:erlang.tuple_to_list(tuple) ++ [idx])
    |> :erlang.list_to_tuple()
  end
end
