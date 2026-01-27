defmodule FDB.LazyRangeIterator do
  @behaviour :erlfdb_iterator

  defstruct [:base, :funs]

  defmodule ListIterator do
    @behaviour :erlfdb_iterator

    def start(list), do: :erlfdb_iterator.start(__MODULE__, list)

    @impl true
    def handle_next([]), do: {:done, []}
    def handle_next([h | t]), do: {:ok, [h], t}

    @impl true
    def handle_stop(_), do: :ok
  end

  def start(tx, start_key, end_key, options \\ []) do
    {:ok, base} = :erlfdb_range_iterator.start(tx, start_key, end_key, options)
    :erlfdb_iterator.start(__MODULE__, %__MODULE__{base: base, funs: []})
  end

  def then(state = %__MODULE__{}, f) do
    %{funs: funs} = state
    %{state | funs: funs ++ [f]}
  end

  def advance(state = %__MODULE__{}, fun \\ &Function.identity/1) do
    %{base: base} = state

    list =
      base
      |> FDB.Stream.from_iterator()
      |> Enum.to_list()

    {fun.(list), %{state | base: ListIterator.start(list)}}
  end

  @impl true
  def handle_next(state = %__MODULE__{}) do
    %{base: base, funs: funs} = state

    case :erlfdb_iterator.next(base) do
      {:ok, [], base} ->
        {:ok, [], %{state | base: base}}

      {:ok, results, base} ->
        state = %{state | base: base}

        case lazy_eval_next(results, funs, []) do
          {:ok, [], funs} ->
            {:ok, [], %{state | funs: funs}}

          {:ok, results, funs} ->
            {:ok, results, %{state | funs: funs}}

          {:done, funs} ->
            {:done, %{state | funs: funs}}
        end

      {:done, base} ->
        {:done, %{state | base: base}}
    end
  end

  @impl true
  def handle_stop(state = %__MODULE__{}) do
    %{base: base} = state
    :erlfdb_iterator.stop(base)
    :ok
  end

  defp lazy_eval_next(results, [], acc), do: {:ok, results, Enum.reverse(acc)}

  defp lazy_eval_next(results, [{fun, state} | funs], acc) do
    case fun.(results, state) do
      {:ok, [], state} ->
        {:ok, [], Enum.reverse([{fun, state} | acc]) ++ funs}

      {:ok, results, state} ->
        lazy_eval_next(results, funs, [{fun, state} | acc])

      {:done, state} ->
        {:done, Enum.reverse([{fun, state} | acc]) ++ funs}
    end
  end
end
