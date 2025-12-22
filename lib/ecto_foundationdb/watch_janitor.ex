defmodule EctoFoundationDB.WatchJanitor do
  @moduledoc """
  A GenServer that cancels watches for a given process upon receiving its `:DOWN` message.

  By registering the watch future with the `WatchJanitor`, we ensure that watches are not
  leaked.

  ## When the WatchJanitor is not used

  Suppose your process exits without canceling the watch. In this case, the watches remain registered in the database.

  - When the watched key eventually changes, the future will be silently resolved and everything will be cleaned up
    automatically at that time.
  - If the watched key never changes, a small amount of memory will remain in use indefinitely. If your application
    creates many watches for keys that never change, you may eventually detect this as a memory leak.
  - By default, each database connection can have no more than 10,000 watches that have not yet reported a change.
    When this number is exceeded, an attempt to create a watch will return a `too_many_watches` error.
  - When your application node exits or is restarted, the watches will remain registered in the database for
    ~15 minutes. At that time, the database will automatically clean them up.

  ## When the WatchJanitor is used

  The above is still true. Additionally, when the process goes down, the `WatchJanitor` will cancel all watches
  registered with it.
  """
  use GenServer

  alias EctoFoundationDB.Future

  @cleanup_interval :timer.minutes(30)
  @cleanup_check_older_than :timer.hours(1)

  defstruct registry: %{},
            cleanup_interval: @cleanup_interval,
            cleanup_check_older_than: @cleanup_check_older_than

  def get!(repo) do
    [{Ecto.Adapters.FoundationDB.Supervisor, sup, :supervisor, _}] =
      Supervisor.which_children(repo)

    repo_children = Supervisor.which_children(sup)
    {__MODULE__, pid, :worker, _} = List.keyfind!(repo_children, __MODULE__, 0)
    pid
  end

  def start_link(opts) do
    config = []
    GenServer.start_link(__MODULE__, [config], opts)
  end

  def start_link(args, opts) do
    GenServer.start_link(__MODULE__, args, opts)
  end

  def register(janitor, pid, futures) do
    GenServer.cast(janitor, {:register, pid, futures})
  end

  def fetch_registered(janitor, pid) do
    GenServer.call(janitor, {:fetch_registered, pid})
  end

  def cleanup(janitor) do
    GenServer.cast(janitor, :cleanup)
  end

  def init([config]) do
    interval = Keyword.get(config, :cleanup_interval, @cleanup_interval)
    check_older_than = Keyword.get(config, :cleanup_check_older_than, @cleanup_check_older_than)
    Process.send_after(self(), :cleanup, interval, [])
    {:ok, %__MODULE__{cleanup_interval: interval, cleanup_check_older_than: check_older_than}}
  end

  def handle_call({:fetch_registered, pid}, _from, state = %__MODULE__{}) do
    {:reply, Map.fetch(state.registry, pid), state}
  end

  def handle_cast(:cleanup, state = %__MODULE__{}) do
    {:noreply, cleanup_all(state)}
  end

  def handle_cast({:register, pid, futures}, state = %__MODULE__{}) do
    %{registry: reg} = state

    now = ts()
    new_futures = for f <- futures, do: {now, f}

    item =
      case Map.fetch(reg, pid) do
        {:ok, item} ->
          %{item | futures: prepend(new_futures, item.futures)}

        :error ->
          mref = Process.monitor(pid)
          %{mref: mref, futures: new_futures}
      end

    reg = Map.put(reg, pid, item)
    {:noreply, cleanup(%{state | registry: reg}, [pid])}
  end

  def handle_info(:cleanup, state = %__MODULE__{}) do
    %{cleanup_interval: interval} = state
    state = cleanup_all(state)
    Process.send_after(self(), :cleanup, interval, [])
    {:noreply, state}
  end

  def handle_info({:DOWN, mref, :process, pid, _reason}, state = %__MODULE__{}) do
    %{registry: reg} = state

    case Map.fetch(reg, pid) do
      {:ok, %{mref: ^mref, futures: futures}} ->
        for {_ts, f} <- futures, do: Future.cancel(f)
        {:noreply, %{state | registry: Map.delete(reg, pid)}}

      _ ->
        {:noreply, state}
    end
  end

  defp ts(), do: :erlang.monotonic_time(:millisecond)

  defp cleanup(state = %__MODULE__{}, pids) do
    %{registry: reg, cleanup_check_older_than: check_older_than} = state

    now = ts()

    reg =
      Enum.reduce(pids, reg, fn pid, reg0 ->
        case Map.fetch(reg, pid) do
          {:ok, item} ->
            case cleanup_item(item, now, check_older_than) do
              {:ok, item} ->
                Map.put(reg, pid, item)

              :error ->
                %{mref: mref} = item
                Process.demonitor(mref)
                Map.delete(reg, pid)
            end

          :error ->
            reg0
        end
      end)

    %{state | registry: reg}
  end

  defp cleanup_all(state = %__MODULE__{}) do
    %{registry: reg, cleanup_check_older_than: check_older_than} = state

    now = ts()

    reg =
      Stream.map(reg, fn {pid, item} ->
        case cleanup_item(item, now, check_older_than) do
          {:ok, item} ->
            {true, {pid, item}}

          :error ->
            %{mref: mref} = item
            Process.demonitor(mref)
            false
        end
      end)
      |> Stream.filter(fn
        {true, _} -> true
        false -> false
      end)
      |> Stream.map(fn {_, entry} -> entry end)
      |> Enum.into(%{})

    %{state | registry: reg}
  end

  defp cleanup_item(item, now, check_older_than) do
    %{futures: futures} = item

    futures =
      futures
      |> Stream.filter(fn {ts, future} ->
        if now - ts >= check_older_than do
          erlfdb_future = Future.erlfdb_future(future)
          !:erlfdb.is_ready(erlfdb_future)
        else
          true
        end
      end)
      |> Enum.to_list()

    case futures do
      [] ->
        :error

      _ ->
        {:ok, Map.put(item, :futures, futures)}
    end
  end

  defp prepend([x], list), do: [x | list]
  defp prepend(list1, list2), do: list1 ++ list2
end
