defmodule EctoFoundationDB.Sync do
  @moduledoc """
  This module defines some conventions for integrating with Phoenix LiveView. Via
  EctoFoundationDB watches, your application can automatically be kept up-to-date with
  changes to the database.

  Simply call one of these functions in `mount/3` or `handle_params/3`, and this module will do the following:

  1. Read from the database and create necessary watches
  1. Call `Phoenix.Component.assign/2`, using the provided `label`
  1. Call `Phoenix.LiveView.attach_hook/4` to set up a callback as a hook

  Then, upon receiving a watch-ready message, the hook calls `Phoenix.Component.assign/2`
  again with the updated data, and creates new watches.

  Note: these functions will store a key called `:ecto_fdb_sync_data` in the `:private` field
  of the provided `socket`.

  ## Examples

  ### Syncing a single record

  Suppose you have a LiveView that displays a single user. You can use `sync_one!/5` to
  automatically update the user whenever it is created, updated, or deleted.

  ```elixir
  defmodule MyApp.UserLive do
    use Phoenix.LiveView

    alias EctoFoundationDB.Sync

    alias MyApp.Repo
    alias MyApp.User

    def mount(_params, _session, socket) do
      user_id = "1"
      {:ok,
        socket
        |> put_private(:tenant, open_tenant(socket))
        |> Sync.sync_one!(Repo, User, :user, user_id)}
    end
  end
  ```

  ### Syncing multiple records

  Suppose you have a LiveView that displays a list of users. You can use `sync_all!/4` to
  automatically update the list whenever a user is created, updated, or deleted.

  You must have already defined a `SchemaMetadata` index for the `User` schema for `sync_all/4`
  to work.

  ```elixir
  defmodule MyApp.UserLive do
    use Phoenix.LiveView

    alias EctoFoundationDB.Sync

    alias MyApp.Repo
    alias MyApp.User

    def mount(_params, _session, socket) do
      {:ok,
        socket
        |> put_private(:tenant, open_tenant(socket))
        |> Sync.sync_all!(Repo, User)}
    end
  end
  ```

  ## Labels

  The `label` argument is used to identify the data being synced. Typically, the `label` is
  simply used as the key in the `assigns` map. It can be any term. Usually, you'll use an
  atom for compatibility with Phoenix.

  For example, you can provide the label `:user` and your assigns map will look like this:

  ```elixir
  iex> assigns
  %{user: %User{
    id: 1,
    name: "Alice",
    email: "alice@example.com"
  }}
  ```

  **Special case:** when `label` is a list, with the first element being an atom, the `label`
  is interpretted by `Sync` as a *path of keys* into nested maps. For example, suppose
  you provide the label `[:users, "alice"]`. The assigns map will be updated as follows:

  ```elixir
  iex> assigns
  %{
    users: %{
      "alice" => %User{
        id: 1,
        name: "Alice",
        email: "alice@example.com"
      }
    }
  }
  ```

  In such a case, empty maps will be created as necessary.

  """

  alias Ecto.Adapters.FoundationDB

  alias EctoFoundationDB.Future
  alias EctoFoundationDB.Indexer.SchemaMetadata
  alias EctoFoundationDB.WatchJanitor

  @data_key :ecto_fdb_sync_data
  @hook_name :ecto_fdb_sync_hook

  defstruct futures: %{}

  @doc """
  Equivalent to `sync_many!/5` with a single record.
  """
  def sync_one!(state, repo, schema, label, id, opts \\ []) do
    sync_many!(state, repo, [{schema, label, id}], opts)
  end

  @doc """
  Initializes a Sync of one or more individual records.

  This is to be paired with `handle_ready/3` to provide automatic updating of `assigns` in `state`. If you're using
  the default LiveView attach_hook as described in the Options, then `handle_ready/3` will be set up for you
  automatically.

  ## Arguments

  - `state`: A map with key `:assigns` and `:private`. `private` must be a map with key `:tenant`
  - `repo`: An Ecto repository
  - `id_assigns`: A list of tuples with schema, label, and id
  - `opts`: Options

  ## Options

  - `assign`: A function that takes the current socket and new assigns and returns the updated state.
    When not provided: if `Phoenix.Component` is available, we use `Phoenix.Component.assign/3`, otherwise we use `Map.put/3`.
  - `attach_hook`: A function that takes `state, name, repo, opts` and modifies state as needed to attach a hook.
    When not provided: if `Phoenix.LiveView` is available, we use `Phoenix.LiewView.attach_hook/4`, otherwise we do nothing.
  - `post_hook`: A function that takes `state, labels` and returns `state`. Executes after the hook. `labels` is a list of
    labels that were assigned. Defaults to a noop

  ## Return

  Returns an updated `state`, with `:assigns` and `:private` updated with the following values:

  ### `assigns`

  - Provided labels from `id_assigns` are used to register the results from `repo.get/3`. For
    any records not found, `nil` is assigned, and no watch is created.

  ### `private`

  - We add or append to the `:ecto_fdb_sync_data`.

  """
  def sync_many!(state, repo, id_assigns, opts \\ []) do
    %{private: private} = state
    %{tenant: tenant} = private

    {new_assigns, new_futures} =
      repo.transactional(
        tenant,
        fn ->
          get_futures =
            for {schema, _label, id} <- id_assigns, do: repo.async_get(schema, id)

          labels = for {_schema, label, _id} <- id_assigns, do: label

          values =
            for value <- repo.await(get_futures) do
              if is_nil(value), do: value, else: FoundationDB.usetenant(value, tenant)
            end

          labeled_values = Enum.zip(labels, values)

          watch_futures =
            for {label, value} <- labeled_values,
                not is_nil(value),
                do: {label, repo.watch(value)}

          {labeled_values, watch_futures}
        end
      )

    state
    |> merge_futures(repo, Enum.into(new_futures, %{}))
    |> apply_assign(new_assigns, opts)
    |> apply_attach_hook(repo, opts)
  end

  @doc """
  Equivalent to `sync_groups!/4` with a single schema.
  """
  def sync_all!(state, repo, queryable, label, opts \\ []) do
    sync_groups!(state, repo, [{queryable, label, []}], opts)
  end

  def sync_all_by!(state, repo, queryable, label, by, opts \\ []) do
    sync_groups!(state, repo, [{queryable, label, by}], opts)
  end

  @doc """
  Initializes a Sync of one or more schemas from a tenant using `EctoFoundationDB.Indexer.SchemaMetadata` for collection tracking.

  This is to be paired with `handle_ready/3` to provide automatic updating of `assigns` in `state`. If you're using
  the default LiveView attach_hook as described in the Options, then `handle_ready/3` will be set up for you
  automatically.

  ## Arguments

  - `state`: A map with key `:assigns` and `:private`. `private` must be a map with key `:tenant`
  - `repo`: An Ecto repository
  - `queryable_assigns`: A list of tuples with `{queryable, label, by}`
  - `opts`: Options

  ## Defining a custom query

  By default, `Repo.all(Schema, prefix: tenant)` or `Repo.all_by(Schema, by, prefix: tenant)` is used to query the database. You can override this by providing your own
  `Ecto.Query` in the `{query, label, by}` tuple. This changes the query used to retrieve data, but does not change the
  watch itself.

  ### Example

  ```elixir
  query = from u in User, order_by: [desc: u.inserted_at]

  {:ok,
   socket
   |> put_private(:tenant, open_tenant(socket))
   |> Sync.sync_all!(:sorted_users, Repo, [{User, :users, [], query}]))}
  ```

  ## Options

  - `assign`: A function that takes the current socket and new assigns and returns the updated state.
    When not provided: if `Phoenix.Component` is available, we use `Phoenix.Component.assign/3`, otherwise we use `Map.put/3`.
  - `attach_hook`: A function that takes `state, name, repo, opts` and modifies state as needed to attach a hook.
    When not provided: if `Phoenix.LiveView` is available, we use `Phoenix.LiewView.attach_hook/4`, otherwise we do nothing.
  - `post_hook`: A function that takes `state` and modifies it as desired after the hook is executed. Defaults to a noop
  - `watch_action`: An atom representing the signal from the `SchemaMetadata` you're interested in syncing. Defaults to `:changes`

  ### `watch_action`

  - `inserts`: Receive signal for each insert or upsert
  - `deletes`: Receive signal for each delete
  - `collection`: Receive signal for each insert, upsert, or delete
  - `updates`: Receive signal for each update (via `Repo.update/*`)
  - `changes`: Receive signal for each insert, upsert, delete, or update

  ## Return

  Returns an updated `state`, with `:assigns` and `:private` updated with the following values:

  ### `assigns`

  - Provided labels from `queryable_assigns` are used to register the results from `repo.all/3`

  ### `private`

  - We add or append to the `:ecto_fdb_sync_data`.

  """
  def sync_groups!(state, repo, queryable_assigns, opts \\ []) do
    %{private: private} = state
    %{tenant: tenant} = private

    watch_action = Keyword.get(opts, :watch_action, :changes)

    {new_assigns, new_futures} =
      repo.transactional(
        tenant,
        fn ->
          get_futures =
            Enum.map(
              queryable_assigns,
              fn
                {queryable, _label, []} ->
                  repo.async_all(queryable)

                {queryable, _label, by} ->
                  repo.async_all_by(queryable, by)
              end
            )

          lists = repo.await(get_futures)

          Enum.zip(queryable_assigns, lists)
          |> Enum.map(fn
            {{queryable, label, by}, list} ->
              list = Enum.map(list, &FoundationDB.usetenant(&1, tenant))
              watch_future = SchemaMetadata.watch_by(queryable, by, watch_action)
              {{label, list}, {label, watch_future}}
          end)
        end
      )
      |> Enum.unzip()

    new_futures = Enum.into(new_futures, %{})

    state
    |> merge_futures(repo, new_futures)
    |> apply_assign(new_assigns, opts)
    |> apply_attach_hook(repo, opts)
  end

  @doc """
  Cancels all syncing and detaches the hook.

  Canceling syncing is optional. EctoFoundationDB will automatically clean up watches when your process exits.

  ## Arguments

  - `state`: A map with key `:assigns` and `:private`. `private` must be a map with key `:tenant`
  - `repo`: An Ecto repository
  - `opts`: Options

  ## Options

  - `detach_hook`: A function that takes `state, name, repo, opts` and modifies state as needed to detach a hook.
    When not provided: if `Phoenix.LiveView` is available, we use `Phoenix.LiewView.detach_hook/3`, otherwise we do nothing.

  ## Return

  Returns an updated `state`, with `:private` updated with the following values:

  ### `private`

  - We cancel and clear the futures in `:ecto_fdb_sync_data`.

  """
  def cancel_all(state, repo, opts \\ []) do
    state
    |> cancel_futures(repo)
    |> apply_detach_hook(repo, opts)
  end

  @doc """
  Cancels syncing for the provided label and, if none are left, detaches the hook.

  Refer to `cancel_all/3` for a discussion on when and why to cancel.

  ## Arguments

  - `state`: A map with key `:assigns` and `:private`. `private` must be a map with key `:tenant`
  - `repo`: An Ecto repository
  - `label`: A label to cancel syncing for
  - `opts`: Options

  ## Options

  - `detach_hook`: A function that takes `state, name, repo, opts` and modifies state as needed to detach a hook.
    When not provided: if `Phoenix.LiveView` is available, we use `Phoenix.LiewView.detach_hook/3`, otherwise we do nothing.

  ## Return

  Returns an updated `state`, with `:private` updated with the following values:

  ### `private`

  - We cancel and clear the futures in `:ecto_fdb_sync_data`.

  """
  def cancel(state, repo, label, opts \\ []) do
    state = cancel_futures(state, repo, [label])
    futures = get_futures(state, repo)

    if map_size(futures) == 0 do
      apply_detach_hook(state, repo, opts)
    else
      state
    end
  end

  @doc """
  This hook can be attached to a compatible Elixir process to automatically
  process handle_info `:ready` messages from EctoFDB.

  This hook is designed to be used with LiveView's `attach_hook`. If you're using
  one of the `sync_*` function in this module, the hook is attached automatically. You
  do not need to call this function.

  ## Arguments

  - `repo`: An Ecto repository.
  - `info`: A message received on the process mailbox. We will inspect messages of the form
     `{ref, :ready} when is_reference(ref)`, and ignore all others (returning `{:cont, state}`).
     Or a list of such messages.
  - `state`: A map with key `:assigns` and `:private`. `private` must be a map with keys `:tenant` and `:ecto_fdb_sync_data`.
  - `opts`: Options

  ## Options

  - `assign`: A function that takes the current socket and new assigns and returns a tuple of new assigns and state.
    By default, we simply update the assigns map with the new labels. The default is not sufficient for LiveView's assign

  ## Result behavior

  Either `{:cont, state}` or `{:halt, state}` is returned.

  - `:cont`: Returned when the message was not processed by the Repo.
  - `:halt`: Returned when the ready message is relevant to the provided
    `futures`. The `assigns` and `private` are updated accordingly based on the label
    provided to the matching future. The watches are re-initialized so that
    the expected syncing behavior will continue.

  """
  def handle_ready(repo, info, state, opts \\ [])

  def handle_ready(repo, {ref, :ready}, state, opts) when is_reference(ref) do
    handle_ready(repo, [{ref, :ready}], state, opts)
  end

  def handle_ready(repo, msg_list, state, opts) when is_list(msg_list) do
    {valid?, refs} =
      Enum.reduce(msg_list, {true, []}, fn
        {ref, :ready}, {true, acc} when is_reference(ref) ->
          {true, [ref | acc]}

        _, {_all?, _acc} ->
          {false, []}
      end)

    refs = Enum.reverse(refs)

    if valid? do
      %{private: private} = state
      %{tenant: tenant} = private
      futures = get_futures(state, repo)

      case repo.assign_ready(futures, refs, watch?: true, prefix: tenant) do
        {[], [], ^futures} ->
          {:cont, state}

        {new_assigns, new_futures, other_futures} ->
          {labels, _} = Enum.unzip(new_assigns)

          {:halt,
           state
           |> put_futures(repo, other_futures)
           |> merge_futures(repo, new_futures)
           |> apply_assign(new_assigns, opts)
           |> apply_post_hook(labels, opts)}
      end
    else
      {:cont, state}
    end
  end

  def handle_ready(_repo, _info, state, _opts) do
    {:cont, state}
  end

  defp apply_assign(state, new_assigns, opts) do
    apply_callback(:assign, [state, new_assigns], opts, fn state, new_assigns ->
      assign(state, new_assigns)
    end)
  end

  defp apply_attach_hook(state, repo, opts) do
    name = get_hook_name(repo)

    apply_callback(:attach_hook, [state, name, repo, opts], opts, fn state, name, repo, opts ->
      attach_hook(state, name, :handle_info, &handle_ready(repo, &1, &2, opts))
    end)
  end

  defp apply_detach_hook(state, repo, opts) do
    name = get_hook_name(repo)

    apply_callback(:detach_hook, [state, name, repo, opts], opts, fn state, name, _repo, _opts ->
      detach_hook(state, name, :handle_info)
    end)
  end

  defp apply_post_hook(state, labels, opts) do
    apply_callback(:post_hook, [state, labels], opts, fn state, _labels ->
      state
    end)
  end

  defp apply_callback(key, args, opts, default) do
    cb = opts[key]

    if is_nil(cb) do
      Kernel.apply(default, args)
    else
      Kernel.apply(cb, args)
    end
  end

  # Optional Phoenix.Component assign behavior
  if Code.ensure_loaded?(Phoenix.Component) do
    def assign_impl(), do: Phoenix.Component

    defp assign(state, new_assigns) do
      new_assigns = create_nested_assigns(new_assigns)
      Phoenix.Component.assign(state, new_assigns)
    end
  else
    def assign_impl(), do: nil

    defp assign(state, new_assigns) do
      assign_map(state, new_assigns)
    end
  end

  def assign_map(state, new_assigns) do
    new_assigns = create_nested_assigns(new_assigns)
    assigns = Map.get(state, :assigns, %{})
    assigns = Map.merge(assigns, Enum.into(new_assigns, %{}))
    Map.put(state, :assigns, assigns)
  end

  defp create_nested_assigns(new_assigns) do
    new_assigns =
      Enum.reduce(new_assigns, [], fn
        {label = [assign_key | rest], value}, acc
        when is_list(label) and is_atom(assign_key) ->
          map = create_nested_assign_map(rest, value)
          [{assign_key, map} | acc]

        {label, value}, acc ->
          [{label, value} | acc]
      end)

    Enum.reverse(new_assigns)
  end

  defp create_nested_assign_map(keys, value) do
    {keys, last} = Enum.split(keys, -1)

    last =
      case last do
        [] -> []
        [last] -> last
      end

    {cumm_k, map} =
      Enum.reduce(keys, {[], %{}}, fn k, {cumm_k, m} ->
        cumm_k = cumm_k ++ [Access.key(k)]
        {cumm_k, update_in(m, cumm_k, &default_map/1)}
      end)

    put_in(map, cumm_k ++ [Access.key(last)], value)
  end

  # Optional Phoenix.LiveView attach_hook/detach_hook behavior
  if Code.ensure_loaded?(Phoenix.LiveView) do
    def hook_impl(), do: Phoenix.LiveView

    defp attach_hook(state, name, event, cb) do
      # Detach, then attach. A future LiveView release may havr `replace: true` option. In the meantime, this is the
      # correct way to replace a hook.
      # https://elixirforum.com/t/complex-components-lead-to-us-always-calling-detach-hook-3-before-attach-hook-4/71233/16?u=jstimps
      state
      |> Phoenix.LiveView.detach_hook(name, event)
      |> Phoenix.LiveView.attach_hook(name, event, cb)
    end

    defp detach_hook(state, name, event) do
      Phoenix.LiveView.detach_hook(state, name, event)
    end
  else
    def hook_impl(), do: nil

    defp attach_hook(state, _name, _event, _cb) do
      state
    end

    defp detach_hook(state, _name, _event) do
      state
    end
  end

  defp get_hook_name(repo) do
    {@hook_name, repo}
  end

  defp get_futures(state, repo) do
    case get_in(state, [Access.key(:private), @data_key, repo, Access.key(:futures)]) do
      nil ->
        %{}

      futures ->
        futures
    end
  end

  defp put_futures(state, repo, futures) do
    state
    |> update_in([Access.key(:private)], &default_map/1)
    |> update_in([Access.key(:private), @data_key], &default_map/1)
    |> update_in([Access.key(:private), @data_key, repo], &default_map/1)
    |> put_in([Access.key(:private), @data_key, repo, Access.key(:futures)], futures)
  end

  defp merge_futures(state, repo, new_futures) do
    WatchJanitor.register(WatchJanitor.get!(repo), self(), Map.values(new_futures))
    futures = get_futures(state, repo)
    futures_to_cancel = Map.intersect(new_futures, futures)
    Enum.each(futures_to_cancel, fn {_, f} -> Future.cancel(f) end)
    put_futures(state, repo, Map.merge(futures, new_futures))
  end

  defp cancel_futures(state, repo) do
    futures_to_cancel = get_futures(state, repo)
    Enum.each(futures_to_cancel, fn {_, f} -> Future.cancel(f) end)
    put_futures(state, repo, %{})
  end

  defp cancel_futures(state, repo, labels) do
    futures = get_futures(state, repo)
    {futures_to_cancel, futures_to_keep} = Map.split(futures, labels)
    Enum.each(futures_to_cancel, fn {_, f} -> Future.cancel(f) end)
    put_futures(state, repo, futures_to_keep)
  end

  defp default_map(nil), do: %{}
  defp default_map(map), do: map
end
