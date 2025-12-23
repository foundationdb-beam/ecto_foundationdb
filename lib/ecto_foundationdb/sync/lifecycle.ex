defmodule EctoFoundationDB.Sync.Lifecycle do
  @moduledoc false
  alias EctoFoundationDB.Sync.State

  def attach_callback(state, repo, name, event = :on_assigns, cb, opts \\ []) do
    callbacks = State.get_callbacks(state, repo)
    event_callbacks = Map.get(callbacks, event, %{})

    named_callbacks =
      case {Keyword.get(opts, :replace, false), Map.fetch(event_callbacks, name)} do
        {false, {:ok, _}} ->
          raise "Callback #{inspect(name)} already exists for #{inspect(repo)}"

        {false, :error} ->
          %{}

        {_, {:ok, cbs}} ->
          cbs
      end

    callbacks = Map.put(callbacks, event, Map.put(named_callbacks, name, cb))
    State.put_callbacks(state, repo, callbacks)
  end

  def detach_callback(state, repo, name, event = :on_assigns, _opts \\ []) do
    callbacks = State.get_callbacks(state, repo)
    event_callbacks = Map.get(callbacks, event, %{})
    event_callbacks = Map.drop(event_callbacks, [name])

    if map_size(event_callbacks) == 0 do
      State.put_callbacks(state, repo, Map.drop(callbacks, [event]))
    else
      State.put_callbacks(state, repo, Map.put(callbacks, event, event_callbacks))
    end
  end
end
