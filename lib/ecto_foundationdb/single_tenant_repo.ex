defmodule EctoFoundationDB.SingleTenantRepo do
  @moduledoc false
  use GenServer

  defstruct [:repo, :tenant_id, :tenant]

  defp get_pid!(repo) do
    [{Ecto.Adapters.FoundationDB.Supervisor, sup, :supervisor, _}] =
      Supervisor.which_children(repo)

    repo_children = Supervisor.which_children(sup)
    {__MODULE__, pid, :worker, _} = List.keyfind!(repo_children, __MODULE__, 0)
    pid
  end

  def start_link(init_arg) do
    GenServer.start_link(__MODULE__, init_arg)
  end

  def get!(repo) do
    GenServer.call(get_pid!(repo), :get)
  end

  def open!(repo) do
    GenServer.call(get_pid!(repo), :open)
  end

  @impl true
  def init([config]) do
    GenServer.cast(self(), :open)
    {:ok, %__MODULE__{repo: config[:repo], tenant_id: config[:tenant_id]}}
  end

  @impl true
  def handle_call(:get, _from, state = %__MODULE__{}) do
    %{tenant: tenant} = state
    {:reply, tenant, state}
  end

  def handle_call(:open, _from, state = %__MODULE__{}) do
    {:noreply, state = %__MODULE__{}} = handle_cast(:open, state)
    %{tenant: tenant} = state
    {:reply, tenant, state}
  end

  @impl true
  def handle_cast(:open, state = %__MODULE__{}) do
    %{repo: repo, tenant_id: id} = state
    tenant = init_single_tenant!(repo, id)
    {:noreply, %{state | tenant: tenant}}
  end

  defp init_single_tenant!(repo, id) do
    EctoFoundationDB.Tenant.open!(repo, id)
  end
end
