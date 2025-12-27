defmodule EctoFoundationDB.Sync.Many do
  @moduledoc false
  @enforce_keys [:label, :schema, :ids]
  defstruct @enforce_keys

  alias EctoFoundationDB.Sync.IdList

  def do_reads(repo, schema, current_idlist, new_ids) do
    current_idmap = for(%{id: id} <- current_idlist, do: {id, true}) |> Enum.into(%{})
    new_idmap = for(id <- new_ids, do: {id, true}) |> Enum.into(%{})
    updates = Map.intersect(current_idmap, new_idmap)
    removes = Map.take(current_idmap, Map.keys(new_idmap))

    futures =
      Enum.map(new_ids, fn id ->
        if Map.has_key?(updates, id) do
          IdList.Markup.keep(id)
        else
          repo.async_get(schema, id)
        end
      end)

    {futures, Map.keys(removes)}
  end
end
