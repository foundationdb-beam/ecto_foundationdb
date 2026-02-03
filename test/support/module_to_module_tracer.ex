defmodule EctoFoundationDB.ModuleToModuleTracer do
  @moduledoc false

  use GenServer

  # module_name
  @type caller() :: atom()

  # {module_name, function_name, arity}
  @type call() :: {atom(), atom(), integer()}

  @type caller_spec() :: function() | caller()
  @type call_spec() :: function() | call()

  @type traced_calls() :: {caller(), call()}

  defstruct caller_specs: [], call_specs: [], traced_calls: []

  @doc """
  The given function is executed and for any function call made within,
  when both a caller_spec and a call_spec are found, the function call is
  recorded in the trace.
  """
  @spec with_traced_calls(atom(), list(caller_spec()), list(call_spec()), function()) ::
          {list(traced_calls()), any()}
  def with_traced_calls(name, caller_specs, call_specs, fun) do
    trace_data = start_trace(name, self(), caller_specs, call_specs)
    res = fun.()
    calls = stop_trace(trace_data)
    {calls, res}
  end

  def start_trace(name, target, caller_specs, call_specs) do
    {:ok, tracer} = start_link(caller_specs, call_specs)

    session = :trace.session_create(name, tracer, [])
    :trace.process(session, target, true, [:call, :arity])

    match_spec = [{:_, [], [{:message, {{:cp, {:caller}}}}]}]

    :trace.function(session, :on_load, match_spec, [:local])

    for {module, function, arity} <- call_specs,
        do: :trace.function(session, {module, function, arity}, match_spec, [:local])

    {tracer, session}
  end

  def stop_trace({tracer, session}) do
    ret = get_traced_calls(tracer)

    :trace.session_destroy(session)

    GenServer.stop(tracer)

    ret
  end

  def start_link(caller_specs, call_specs) do
    GenServer.start_link(__MODULE__, {caller_specs, call_specs}, [])
  end

  def get_traced_calls(pid) do
    GenServer.call(pid, :get_traced_calls)
  end

  @impl true
  def init({caller_specs, call_specs}) do
    {:ok, %__MODULE__{caller_specs: caller_specs, call_specs: call_specs}}
  end

  @impl true
  def handle_call(:get_traced_calls, _from, state) do
    {:reply, Enum.reverse(state.traced_calls), state}
  end

  @impl true
  def handle_info(
        {:trace, _pid, :call, call = {_module, _fun, _arity}, {:cp, {caller, _, _}}},
        state = %__MODULE__{}
      ) do
    if match?(caller, call, state) do
      {:noreply, %{state | traced_calls: [{caller, call} | state.traced_calls]}}
    else
      {:noreply, state}
    end
  end

  def handle_info(_info, state) do
    # other traces will end up here
    {:noreply, state}
  end

  defp match?(caller, call, state) do
    matching_origin?(caller, state) and matching_call?(call, state)
  end

  defp matching_origin?(caller, state) do
    any_match?(state.caller_specs, caller)
  end

  defp matching_call?(call, state) do
    any_match?(state.call_specs, call)
  end

  defp any_match?(specs, item) do
    Enum.any?(
      specs,
      fn
        spec when is_function(spec) ->
          spec.(item)

        spec ->
          item == spec
      end
    )
  end
end
