defmodule EctoFoundationDB.Sync.IdList do
  @moduledoc false
  defmodule Markup do
    @moduledoc false
    defstruct [:idlist]
    def new(idlist), do: %Markup{idlist: idlist}
    def keep(id), do: {:keep, id}
    def remove(id), do: {:remove, id}
    def add_head(markup, item), do: %Markup{idlist: [item | markup.idlist]}
    def reverse(markup), do: %Markup{idlist: Enum.reverse(markup.idlist)}
  end

  # the order of 'b' is preferred, we allow data from a to move over to b
  # b can contain data or markers
  def replace(a, %Markup{idlist: b}) when is_list(a) do
    {am, _, _} = to_map(a)

    b
    |> Enum.reduce([], fn
      {:keep, id}, b0 ->
        [Map.get(am, id) | b0]

      {:remove, _id}, b0 ->
        b0

      data = %{id: _id}, b0 ->
        [data | b0]
    end)
    |> Enum.reverse()
  end

  # the order of 'a' is preferred. We allow b to remove items from a
  # we return the data from a that was removed from a
  # b can contain data or markers
  def merge(a, %Markup{idlist: b}) when is_list(a) do
    {bm, _keeps, removes} = to_map(b)

    # Move items from b into a by iterating over a
    {bm, a_rev, removes_rev} = Enum.reduce(a, {bm, [], []}, &merge_reducer(removes, &1, &2))

    # Add in the rest of the items from b not yet handled, and in the order of b
    b =
      Enum.filter(b, fn
        %{id: id} ->
          Map.has_key?(bm, id)

        _ ->
          false
      end)

    {Enum.reverse(a_rev) ++ b, Enum.reverse(removes_rev)}
  end

  defp merge_reducer(removes, data = %{id: id}, {bm0, a0, r0}) do
    case Map.fetch(bm0, id) do
      {:ok, new_data} ->
        {Map.delete(bm0, id), [new_data | a0], r0}

      :error ->
        case Map.fetch(removes, id) do
          {:ok, true} ->
            {bm0, a0, [data | r0]}

          :error ->
            {bm0, [data | a0], r0}
        end
    end
  end

  def to_map(l) do
    Enum.reduce(l, {%{}, %{}, %{}}, fn
      data = %{id: id}, {m, keeps, removes} ->
        {Map.put(m, id, data), keeps, removes}

      {:keep, id}, {m, keeps, removes} ->
        {m, Map.put(keeps, id, true), removes}

      {:remove, id}, {m, keeps, removes} ->
        {m, keeps, Map.put(removes, id, true)}
    end)
  end
end
