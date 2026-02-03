defmodule FDB.LazyRangeIterator do
  @moduledoc false
  @behaviour :erlfdb_iterator

  defstruct [:base, :funs]

  defmodule ListIterator do
    @moduledoc false
    @behaviour :erlfdb_iterator

    def start(list), do: :erlfdb_iterator.new(__MODULE__, list)

    @impl true
    def handle_next([]), do: {:halt, []}
    def handle_next([h | t]), do: {:cont, [h], t}

    @impl true
    def handle_stop(_), do: :ok
  end

  def start(tx, start_key, end_key, options \\ []) do
    base = :erlfdb_range_iterator.start(tx, start_key, end_key, options)
    :erlfdb_iterator.new(__MODULE__, %__MODULE__{base: base, funs: []})
  end

  def then(iterator, f, f_state) do
    {:ok, iterator} = :erlfdb_iterator.call(iterator, {:then, f, f_state})
    iterator
  end

  def advance(iterator, fun \\ &Function.identity/1) do
    :erlfdb_iterator.call(iterator, {:advance, fun})
  end

  @impl true
  def handle_call({:advance, fun}, _, state = %__MODULE__{}) do
    %{base: base} = state

    list =
      base
      |> FDB.Stream.from_iterator()
      |> Enum.to_list()

    {:reply, fun.(list), %{state | base: ListIterator.start(list)}}
  end

  def handle_call({:then, f, f_state}, _, state = %__MODULE__{}) do
    %{funs: funs} = state
    {:reply, :ok, %{state | funs: funs ++ [{f, f_state}]}}
  end

  @impl true
  def handle_next(state = %__MODULE__{}) do
    %{base: base} = state

    case :erlfdb_iterator.next(base) do
      {base_status, [], base} when base_status in [:cont, :halt] ->
        {base_status, [], %{state | base: base}}

      {base_status, results, base} when base_status in [:cont, :halt] ->
        handle_results(base_status, results, %{state | base: base})

      {:halt, base} ->
        {:halt, %{state | base: base}}
    end
  end

  @impl true
  def handle_stop(state = %__MODULE__{}) do
    %{base: base} = state
    :erlfdb_iterator.stop(base)
    :ok
  end

  defp handle_results(base_status, results, state) do
    %{funs: funs} = state

    case lazy_eval_next(results, funs, []) do
      {:cont, [], funs} ->
        {base_status, [], %{state | funs: funs}}

      {:cont, results, funs} ->
        {base_status, results, %{state | funs: funs}}

      {:halt, results, funs} ->
        {:halt, results, %{state | funs: funs}}

      {:halt, funs} ->
        {:halt, %{state | funs: funs}}
    end
  end

  defp lazy_eval_next(results, [], acc), do: {:cont, results, Enum.reverse(acc)}

  defp lazy_eval_next(results, [{fun, state} | funs], acc) do
    case fun.(results, state) do
      {:cont, [], state} ->
        {:cont, [], Enum.reverse([{fun, state} | acc]) ++ funs}

      {:cont, results, state} ->
        lazy_eval_next(results, funs, [{fun, state} | acc])

      {:halt, results, state} ->
        {:halt, results, Enum.reverse([{fun, state} | acc]) ++ funs}

      {:halt, state} ->
        {:halt, Enum.reverse([{fun, state} | acc]) ++ funs}
    end
  end
end
