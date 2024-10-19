defmodule EctoFoundationDB.Sandbox.Sandboxer do
  @moduledoc false
  use GenServer

  defstruct [:db]

  def start_link(init_arg) do
    GenServer.start_link(__MODULE__, init_arg)
  end

  def get_or_create_test_db(pid) do
    GenServer.call(pid, :get_or_create_test_db, 60_000)
  end

  @impl true
  def init(_init_arg) do
    {:ok, %__MODULE__{}}
  end

  @impl true
  def handle_call(:get_or_create_test_db, _from, state = %__MODULE__{db: nil}) do
    # :erlfdb_util.get_test_db/1 has a wide-open receive block, so we have to insulate
    # it from the GenServer
    task = Task.async(fn -> :erlfdb_util.get_test_db([]) end)
    db = Task.await(task)

    {:reply, db, %__MODULE__{state | db: db}}
  end

  def handle_call(:get_or_create_test_db, _from, state = %__MODULE__{db: db}) do
    {:reply, db, state}
  end
end
