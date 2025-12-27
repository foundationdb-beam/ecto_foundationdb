defmodule EctoFoundationDB.Sync.Lifecycle do
  @moduledoc false
  alias EctoFoundationDB.Sync.State

  def attach_callback(state, repo, name \\ :default, event = :handle_assigns, cb, opts \\ []) do
    callbacks = State.get_callbacks(state, repo)
    event_callbacks = Map.get(callbacks, event, [])

    event_callbacks =
      case {Keyword.get(opts, :replace, false), List.keyfind(event_callbacks, name, 0, nil)} do
        {false, ecb} when not is_nil(ecb) ->
          raise "Callback #{inspect(name)} already exists for #{inspect(repo)}"

        _ ->
          List.keydelete(event_callbacks, name, 0)
      end

    event_callbacks = List.keystore(event_callbacks, name, 0, {name, cb})

    callbacks = Map.put(callbacks, event, event_callbacks)
    State.put_callbacks(state, repo, callbacks)
  end

  def detach_callback(state, repo, name \\ :default, event = :handle_assigns, _opts \\ []) do
    callbacks = State.get_callbacks(state, repo)
    event_callbacks = Map.get(callbacks, event, %{})
    event_callbacks = List.keydelete(event_callbacks, name, 0)

    if Enum.empty?(event_callbacks) do
      State.put_callbacks(state, repo, Map.drop(callbacks, [event]))
    else
      State.put_callbacks(state, repo, Map.put(callbacks, event, event_callbacks))
    end
  end
end
