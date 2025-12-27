defmodule EctoFoundationDB.Sync do
  @moduledoc """
  This module defines some conventions for integrating with Phoenix LiveView or any other stateful
  process. Via EctoFoundationDB watches, your application can automatically be kept up-to-date with
  changes to the database.

  Simply call one of the provided `sync*` functions in `mount/3` or `handle_params/3`, and this module will do the following:

  1. Read from the database and create necessary watches
  1. Call `Phoenix.Component.assign/2`, using the provided `label`
  1. Call `Phoenix.LiveView.attach_hook/4` to set up a callback as a hook

  Then, upon receiving a watch-ready message, the hook calls `Phoenix.Component.assign/2`
  again with the updated data, and creates new watches as needed.

  Socket requirements:

  * The tenant must be stored in the `:private` field of the provided `socket`.
  * The sync functions will store a key called `:ecto_fdb_sync_data` in the `:private` field.

  ## Examples

  These are quick, short examples. Please see [Sync Engine III](sync_module.html) for end-to-end detail.

  ### Quick example 1: Syncing a single record

  Suppose you have a LiveView that displays a single user. You can use `sync_one/5` to
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
        |> Sync.sync_one(Repo, User, :user, user_id)}
    end
  end
  ```

  ### Quick example 2: Syncing a list of records

  Suppose you have a LiveView that displays a list of users. You can use `sync_all/4` to
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
        |> Sync.sync_all(Repo, User)}
    end
  end
  ```

  ### Quick example 3: Syncing a group of records indivudually

  Suppose your page displays serveral records simulataneously, but you wish to subscribe to change individually.
  You can use `sync_many/6`. This integrates nicely with LiveComponents.

  ```elixir
  defmodule MyApp.UserLive do
    use Phoenix.LiveView

    alias EctoFoundationDB.Sync

    alias MyApp.Repo
    alias MyApp.User

    def mount(_params, _session, socket) do
      user_ids = ["1", "2", "3"]
      {:ok,
        socket
        |> put_private(:tenant, open_tenant(socket))
        |> Sync.sync_many(Repo, User, :users, user_ids)}
    end
  end
  ```

  ## Labels

  The `label` argument is used to identify the data being synced. The `label` is
  used as the key in the `assigns` map. It can be any term. Usually, you'll use an
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
  """

  alias Ecto.Adapters.FoundationDB

  alias EctoFoundationDB.Future
  alias EctoFoundationDB.Indexer.SchemaMetadata
  alias EctoFoundationDB.Sync.All
  alias EctoFoundationDB.Sync.IdList
  alias EctoFoundationDB.Sync.Lifecycle
  alias EctoFoundationDB.Sync.Many
  alias EctoFoundationDB.Sync.One
  alias EctoFoundationDB.Sync.State

  @container_hook_name :ecto_fdb_sync_hook

  defstruct futures: %{}

  @doc """
  Sets up syncing for a single record in the database.

  See `sync/4` for more.
  """
  def sync_one(state, repo, label, schema, id, opts \\ []) do
    sync(state, repo, [%One{label: label, schema: schema, id: id}], opts)
  end

  @doc """
  Sets up syncing for a list of records in the database, with indivudual watches.

  See `sync/4` for more.
  """
  def sync_many(state, repo, label, schema, ids, opts \\ [])

  def sync_many(state, repo, label, _schema, cancel, opts) when cancel in [nil, []] do
    cancel(state, repo, label, opts)
  end

  def sync_many(state, repo, label, schema, ids, opts) do
    sync(state, repo, [%Many{label: label, schema: schema, ids: ids}], opts)
  end

  @doc """
  Sets up syncing for a list of records in the database.

  A watch is created for changes to the list with SchemaMetadata.

  See `sync/4` for more.

  ## Options

  - `watch_action`: An atom representing the signal from the `SchemaMetadata` you're interested in syncing. Defaults to `:changes`
  """
  def sync_all(state, repo, label, queryable, opts \\ []) do
    sync_all_by(state, repo, label, queryable, [], opts)
  end

  @doc """
  Sets up syncing for a list of records in the database, constrained by an indexed field.

  A watch is created for changes to the list with SchemaMetadata.

  See `sync/4` for more.

  ## Options

  - `watch_action`: An atom representing the signal from the `SchemaMetadata` you're interested in syncing. Defaults to `:changes`
  """
  def sync_all_by(state, repo, label, queryable, by, opts \\ []) do
    watch_action = Keyword.get(opts, :watch_action, :changes)
    entry = %All{label: label, queryable: queryable, by: by, watch_action: watch_action}
    sync(state, repo, [entry], opts)
  end

  @doc """
  Initializes a Sync of one or more queryables.

  This is to be paired with `handle_ready/3` to provide automatic updating of `assigns` in `state`. If you're using
  the default LiveView attach_hook as described in the Options, then `handle_ready/3` will be set up for you
  automatically.

  ## Arguments

  - `state`: A map with key `:assigns` and `:private`. `private` must be a map with key `:tenant`
  - `repo`: An Ecto repository
  - `queryable_assigns`: A list of `All`, `One`, or `Many` structs
  - `opts`: Options

  ## Options

  - `assign`: A function that takes the current socket and new assigns and returns the updated state.
    When not provided: if `Phoenix.Component` is available, we use `Phoenix.Component.assign/3`, otherwise we use `Map.put/3`.
  - `attach_container_hook`: A function that takes `state, name, repo, opts` and modifies state as needed to attach a hook.
    When not provided: if `Phoenix.LiveView` is available, we use `Phoenix.LiewView.attach_hook/4`, otherwise we do nothing.

  ## Return

  Returns an updated `state`, with `:assigns` and `:private` updated with the following values:

  ### `assigns`

  - Provided labels from `queryable_assigns` are used to register the results from the database.

  ### `private`

  - We add or append to the `:ecto_fdb_sync_data` as needed for internal purposes.

  """
  def sync(state, repo, queryable_assigns, opts \\ []) do
    %{private: private} = state
    %{tenant: tenant} = private

    {std_assigns, std_watches, idlist_assigns, idlist_watches, cancellations} =
      repo.transactional(
        tenant,
        fn ->
          {std_labeled_futures, idlist_futures, cancellations} =
            do_reads(state, repo, queryable_assigns, {[], [], []})

          {std_q, std_futures} = Enum.unzip(std_labeled_futures)
          std_results = repo.await(std_futures)
          std_results = Enum.zip(std_q, std_results)

          # get_range only exists in 'std' path, so there's no performance benefit in a single overall await
          idlist_results =
            Enum.map(idlist_futures, fn {q, fl} ->
              {q,
               Enum.map(fl, fn
                 future = %Future{} -> repo.await(future)
                 x -> x
               end)}
            end)

          {std_assigns, std_watches} = do_watches(repo, std_results, tenant, {[], %{}})
          {idlist_assigns, idlist_watches} = do_watches(repo, idlist_results, tenant, {[], %{}})

          {std_assigns, std_watches, idlist_assigns, idlist_watches, cancellations}
        end
      )

    # Safe to skip empty assigns since we're about to assign to a new list
    {_, state} = State.cancel_futures(state, repo, :idlist, cancellations)

    state
    |> State.update_futures(repo, :std, std_watches)
    |> State.update_futures(repo, :idlist, idlist_watches)
    |> apply_attach_container_hook(repo, opts)
    |> apply_assign(repo, std_assigns, idlist_assigns, [{:idlist, :replace} | opts])
  end

  defp do_reads(_state, _repo, [], {std_assigns, idlist_assigns, cancellations}) do
    {Enum.reverse(std_assigns), Enum.reverse(idlist_assigns),
     List.flatten(Enum.reverse(cancellations))}
  end

  defp do_reads(
         state,
         repo,
         [q = %Many{label: label, schema: schema, ids: ids} | queryable_assigns],
         {std_assigns, idlist_assigns, cancellations}
       ) do
    assigns = Map.get(state, :assigns, %{})
    existing = Map.get(assigns, label, [])
    {futures, to_cancel} = Many.do_reads(repo, schema, existing, ids)
    entry = {q, futures}
    to_cancel = for id <- to_cancel, do: {label, id}
    acc = {std_assigns, [entry | idlist_assigns], [to_cancel | cancellations]}
    do_reads(state, repo, queryable_assigns, acc)
  end

  defp do_reads(
         state,
         repo,
         [q = %One{schema: schema, id: id} | queryable_assigns],
         {std_assigns, idlist_assigns, cancellations}
       ) do
    entry = {q, repo.async_get(schema, id)}
    acc = {[entry | std_assigns], idlist_assigns, cancellations}
    do_reads(state, repo, queryable_assigns, acc)
  end

  defp do_reads(
         state,
         repo,
         [q = %All{queryable: queryable, by: []} | queryable_assigns],
         {std_assigns, idlist_assigns, cancellations}
       ) do
    entry = {q, repo.async_all(queryable)}
    acc = {[entry | std_assigns], idlist_assigns, cancellations}
    do_reads(state, repo, queryable_assigns, acc)
  end

  defp do_reads(
         state,
         repo,
         [q = %All{queryable: queryable, by: by} | queryable_assigns],
         {std_assigns, idlist_assigns, cancellations}
       ) do
    entry = {q, repo.async_all_by(queryable, by)}
    acc = {[entry | std_assigns], idlist_assigns, cancellations}
    do_reads(state, repo, queryable_assigns, acc)
  end

  defp do_watches(_repo, [], _tenant, {acc_assigns, acc_watches}),
    do: {Enum.reverse(acc_assigns), acc_watches}

  defp do_watches(
         repo,
         [{%Many{label: label}, values} | results],
         tenant,
         {acc_assigns, acc_watches}
       ) do
    values = usetenant(values, tenant)
    values = Enum.filter(values, &(&1 != nil))

    watch_futures =
      Stream.map(values, fn
        val when is_struct(val) ->
          {true, {{label, val.id}, repo.watch(val)}}

        _ ->
          false
      end)
      |> Stream.filter(fn
        {true, _} -> true
        _ -> false
      end)
      |> Stream.map(fn {true, x} -> x end)
      |> Enum.into(%{})

    values = IdList.Markup.new(values)
    acc = {[{label, values} | acc_assigns], Map.merge(acc_watches, watch_futures)}
    do_watches(repo, results, tenant, acc)
  end

  defp do_watches(
         repo,
         [{%One{label: label}, value} | results],
         tenant,
         {acc_assigns, acc_watches}
       ) do
    value = usetenant(value, tenant)

    acc =
      if is_nil(value) do
        {[{label, value} | acc_assigns], acc_watches}
      else
        watch_future = repo.watch(value)
        {[{label, value} | acc_assigns], Map.put(acc_watches, label, watch_future)}
      end

    do_watches(repo, results, tenant, acc)
  end

  defp do_watches(
         repo,
         [
           {%All{label: label, queryable: queryable, by: by, watch_action: watch_action}, values}
           | results
         ],
         tenant,
         {acc_assigns, acc_watches}
       ) do
    values = usetenant(values, tenant)
    watch_future = SchemaMetadata.watch_by(queryable, by, watch_action)
    acc = {[{label, values} | acc_assigns], Map.put(acc_watches, label, watch_future)}
    do_watches(repo, results, tenant, acc)
  end

  def watching?(state, repo, label) do
    futures = State.get_futures(state, repo)
    std = Map.get(futures, :std, %{})
    idlist = Map.get(futures, :idlist, %{})
    Map.has_key?(std, label) or Map.has_key?(idlist, label)
  end

  @doc """
  Attaches a callback to the `:handle_assigns` event.

  The callback will be called when the `Sync` module changes your assigns.

  ## Arguments

  - `state`: A map with key `:assigns` and `:private`. `private` must be a map with key `:tenant`
  - `repo`: An Ecto repository
  - `name`: The name of the callback. Defaults to `:default`.
  - `event`: The event to attach the callback to. Only `:handle_assigns` is supported.
  - `cb`: The callback function. Arity must be 2, with the first argument being the `state` and
    the second being a map with the old assigns.
  - `opts`: Options

  ## Options

  - `:replace`: A boolean indicating whether to replace an existing callback with the same name and event. Defaults to `false`.
  """
  defdelegate attach_callback(state, repo, name \\ :default, event, cb, opts \\ []), to: Lifecycle

  @doc """
  Detaches a callback from the `:handle_assigns` event.
  """
  defdelegate detach_callback(state, repo, name \\ :default, event, opts \\ []), to: Lifecycle

  @doc """
  Cancels all syncing and detaches the hook.

  Canceling syncing is optional. EctoFoundationDB will automatically clean up watches when your process exits.

  ## Arguments

  - `state`: A map with key `:assigns` and `:private`. `private` must be a map with key `:tenant`
  - `repo`: An Ecto repository
  - `opts`: Options

  ## Options

  - `detach_container_hook`: A function that takes `state, name, repo, opts` and modifies state as needed to detach a container hook.
    When not provided: if `Phoenix.LiveView` is available, we use `Phoenix.LiewView.detach_hook/3`, otherwise we do nothing.

  ## Return

  Returns an updated `state`, with `:private` updated with the following values:

  ### `private`

  - We cancel and clear the futures in `:ecto_fdb_sync_data`.

  """
  def cancel_all(state, repo, opts \\ []) do
    {std_labels, state} = State.cancel_futures(state, repo, :std)
    {idlist_labels, state} = State.cancel_futures(state, repo, :idlist)

    state =
      if Keyword.get(opts, :assign, true) do
        nil_assigns = for l <- std_labels, do: {l, nil}
        empty_assigns = for l <- idlist_labels, do: {l, IdList.Markup.new([])}
        apply_assign(state, repo, nil_assigns, empty_assigns, [{:idlist, :replace} | opts])
      else
        state
      end

    apply_detach_container_hook(state, repo, opts)
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

  - `assign`: A boolean indicating whether or not to assign the label to `nil` or `[]`. Defaults to `true`.
  - `detach_container_hook`: A function that takes `state, name, repo, opts` and modifies state as needed to detach a container hook.
    When not provided: if `Phoenix.LiveView` is available, we use `Phoenix.LiewView.detach_hook/3`, otherwise we do nothing.

  ## Return

  Returns an updated `state`, with `:private` updated with the following values:

  ### `private`

  - We cancel and clear the futures in `:ecto_fdb_sync_data`.

  """
  def cancel(state, repo, label, opts \\ []), do: cancel_(state, repo, [label], opts)

  defp cancel_(state, repo, labels, opts) do
    {std_labels, state} = State.cancel_futures(state, repo, :std, labels)
    {idlist_labels, state} = State.cancel_futures(state, repo, :idlist, labels)

    state =
      if Keyword.get(opts, :assign, true) do
        nil_assigns = for l <- std_labels, do: {l, nil}
        empty_assigns = for l <- idlist_labels, do: {l, IdList.Markup.new([])}
        apply_assign(state, repo, nil_assigns, empty_assigns, [{:idlist, :replace} | opts])
      else
        state
      end

    if State.futures_empty?(state, repo) do
      apply_detach_container_hook(state, repo, opts)
    else
      state
    end
  end

  @doc """
  This hook can be attached to a compatible Elixir process to automatically
  process handle_info `:ready` messages from EctoFDB.

  This hook is designed to be used with LiveView's `attach_hook`. If you're using
  one of the `sync_*` function in this module along with LiveView, the hook is
  attached automatically. You do not need to call this function.

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

  def handle_ready(repo, msg = {ref, :ready}, state, opts) when is_reference(ref) do
    case assign_ready_(state, repo, :std, msg) do
      {:cont, state} ->
        case assign_ready_(state, repo, :idlist, msg) do
          {:cont, state} ->
            {:cont, state}

          {:halt, idlist_assigns, state} ->
            idlist_assigns = get_idlist_assigns_for_merge(idlist_assigns)

            {:halt, apply_assign(state, repo, [], idlist_assigns, [{:idlist, :merge} | opts])}
        end

      {:halt, std_assigns, state} ->
        {:halt, apply_assign(state, repo, std_assigns, [], [{:idlist, :merge} | opts])}
    end
  end

  def handle_ready(_repo, _info, state, _opts) do
    {:cont, state}
  end

  defp assign_ready_(state, repo, key, {ready_ref, :ready}) do
    %{private: private} = state
    %{tenant: tenant} = private
    futures = State.get_futures(state, repo)
    future_map = Map.get(futures, key, %{})

    empty_map = %{}

    case repo.assign_ready(future_map, [ready_ref], watch?: true, prefix: tenant) do
      {[], ^empty_map, ^future_map} ->
        {:cont, state}

      {new_assigns, new_f, rem_f} ->
        {:halt, new_assigns, State.merge_futures(state, repo, key, rem_f, new_f)}
    end
  end

  defp get_idlist_assigns_for_merge(idlist_assigns) do
    # the assign_ready function uses the label from the futures map, so we
    # transform {{label, id}, value} into {label, [value, ...]}
    Enum.reduce(idlist_assigns, %{}, fn
      {{label, id}, nil}, acc ->
        list = Map.get(acc, label, IdList.Markup.new([]))
        list = IdList.Markup.add_head(list, IdList.Markup.remove(id))
        Map.put(acc, label, list)

      {{label, _id}, value}, acc ->
        list = Map.get(acc, label, IdList.Markup.new([]))
        list = IdList.Markup.add_head(list, value)
        Map.put(acc, label, list)
    end)
    |> Enum.into([])
  end

  defp apply_assign(state, _repo, [], [], _opts) do
    state
  end

  defp apply_assign(state, repo, std_assigns, idlist_assigns, opts) do
    {std_labels, _} = Enum.unzip(std_assigns)
    {idlist_labels, _} = Enum.unzip(idlist_assigns)

    old_assigns = get_old_assigns(state, std_labels, idlist_labels)

    state =
      apply_callback(:assign, [state, std_assigns, idlist_assigns, opts], opts, fn state,
                                                                                   std_assigns,
                                                                                   idlist_assigns,
                                                                                   opts ->
        assign(state, std_assigns, idlist_assigns, opts)
      end)

    {_, state} =
      state
      |> State.get_callbacks(repo)
      |> Map.get(:handle_assigns, %{})
      |> Enum.reduce(
        {:cont, state},
        fn
          {_name, cb}, {:cont, state0} ->
            cb.(state0, old_assigns)

          _, {:halt, state0} ->
            {:halt, state0}
        end
      )

    state
  end

  defp apply_attach_container_hook(state, repo, opts) do
    name = get_hook_name(repo)

    apply_callback(:attach_container_hook, [state, name, repo, opts], opts, fn state,
                                                                               name,
                                                                               repo,
                                                                               opts ->
      attach_container_hook(state, name, :handle_info, &handle_ready(repo, &1, &2, opts))
    end)
  end

  defp apply_detach_container_hook(state, repo, opts) do
    name = get_hook_name(repo)

    apply_callback(:detach_container_hook, [state, name, repo, opts], opts, fn state,
                                                                               name,
                                                                               _repo,
                                                                               _opts ->
      detach_container_hook(state, name, :handle_info)
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

    defp assign(state, std_assigns, idlist_assigns, opts) do
      state.assigns
      |> assign_map(std_assigns, idlist_assigns, opts)
      |> then(&Phoenix.Component.assign(state, &1))
    end
  else
    def assign_impl(), do: nil

    defp assign(state, std_assigns, idlist_assigns, opts) do
      assigns = Map.get(state, :assigns, %{})
      new_assigns_map = assign_map(assigns, std_assigns, idlist_assigns, opts)
      Map.put(state, :assigns, Map.merge(assigns, new_assigns_map))
    end
  end

  def assign_map(assigns, std_assigns, idlist_assigns, opts \\ []) do
    idlist_assigns_map =
      Enum.reduce(idlist_assigns, %{}, fn {label, new_item_list}, acc ->
        old_item_list = Map.get(assigns, label, [])

        item_list =
          case Keyword.get(opts, :idlist, :merge) do
            :merge ->
              # removes can be ignored on merge since they come from the assign_ready, which
              # handles the removal of futures that no longer have a data object
              {result, _removes} = IdList.merge(old_item_list, new_item_list)
              result

            :replace ->
              IdList.replace(old_item_list, new_item_list)
          end

        Map.put(acc, label, item_list)
      end)

    Map.merge(idlist_assigns_map, Enum.into(std_assigns, %{}))
  end

  # Optional Phoenix.LiveView attach_hook/detach_hook behavior
  if Code.ensure_loaded?(Phoenix.LiveView) do
    def hook_impl(), do: Phoenix.LiveView

    defp attach_container_hook(state, name, event, cb) do
      # Detach, then attach. A future LiveView release may havr `replace: true` option. In the meantime, this is the
      # correct way to replace a hook.
      # https://elixirforum.com/t/complex-components-lead-to-us-always-calling-detach-hook-3-before-attach-hook-4/71233/16?u=jstimps
      state
      |> Phoenix.LiveView.detach_hook(name, event)
      |> Phoenix.LiveView.attach_hook(name, event, cb)
    end

    defp detach_container_hook(state, name, event) do
      Phoenix.LiveView.detach_hook(state, name, event)
    end
  else
    def hook_impl(), do: nil

    defp attach_container_hook(state, _name, _event, _cb) do
      state
    end

    defp detach_container_hook(state, _name, _event) do
      state
    end
  end

  defp get_hook_name(repo) do
    {@container_hook_name, repo}
  end

  defp usetenant(nil, _tenant), do: nil

  defp usetenant(list, tenant) when is_list(list) do
    Enum.map(
      list,
      fn
        struct when is_struct(struct) ->
          FoundationDB.usetenant(struct, tenant)

        data ->
          data
      end
    )
  end

  defp usetenant(val, tenant) do
    FoundationDB.usetenant(val, tenant)
  end

  defp get_old_assigns(state, std_labels, idlist_labels) do
    assigns = Map.get(state, :assigns, %{})

    Map.merge(
      take_with_default(assigns, std_labels, nil),
      take_with_default(assigns, idlist_labels, [])
    )
  end

  defp take_with_default(map, keys, default) do
    Enum.reduce(keys, %{}, fn key, acc ->
      map
      |> Map.get(key, default)
      |> then(&Map.put(acc, key, &1))
    end)
  end
end
