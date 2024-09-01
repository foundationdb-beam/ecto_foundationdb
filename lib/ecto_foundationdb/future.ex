defmodule EctoFoundationDB.Future do
  @moduledoc false
  alias EctoFoundationDB.Layer.Tx
  defstruct [:schema, :ref, :result, :handler, :must_wait?, :all_or_one]

  @token :__ectofdbfuture__

  def token(), do: @token

  def before_transactional(schema) do
    %__MODULE__{schema: schema, handler: &Function.identity/1, must_wait?: not Tx.in_tx?()}
  end

  def schema(%__MODULE__{schema: schema}), do: schema

  def set_all_or_one(fut, hint), do: %__MODULE__{fut | all_or_one: hint}
  def all_or_one(%__MODULE__{all_or_one: hint}), do: hint

  def new(schema) do
    %__MODULE__{schema: schema, handler: &Function.identity/1, must_wait?: true}
  end

  def set(fut, future_ref, f \\ &Function.identity/1) do
    %__MODULE__{handler: g} = fut
    %__MODULE__{fut | ref: future_ref, handler: &f.(g.(&1))}
  end

  def leaving_transactional(fut = %__MODULE__{must_wait?: true}) do
    %__MODULE__{fut | ref: nil, result: result(fut), handler: &Function.identity/1}
  end

  def leaving_transactional(fut = %__MODULE__{must_wait?: false}) do
    fut
  end

  def result(fut = %__MODULE__{ref: nil, handler: f}) do
    f.(fut.result)
  end

  def result(fut) do
    %__MODULE__{ref: ref, handler: handler} = fut
    handler.(:erlfdb.wait(ref))
  end

  def results(futs) do
    # important to maintain order of the input futures
    refs = for %__MODULE__{ref: ref} <- futs, not is_nil(ref), do: ref
    results = Enum.zip(refs, :erlfdb.wait_for_all(refs)) |> Enum.into(%{})

    Enum.map(
      futs,
      fn fut = %__MODULE__{ref: ref, result: result, handler: f} ->
        case Map.get(results, ref, nil) do
          nil ->
            %__MODULE__{fut | ref: nil, result: f.(result), handler: &Function.identity/1}

          new_result ->
            %__MODULE__{fut | ref: nil, result: f.(new_result), handler: &Function.identity/1}
        end
      end
    )
  end

  def apply(fut = %__MODULE__{ref: nil}, f) do
    %__MODULE__{handler: g, result: result} = fut
    %__MODULE__{result: f.(g.(result)), handler: &Function.identity/1}
  end

  def apply(fut, f) do
    %__MODULE__{handler: g} = fut
    %__MODULE__{fut | handler: &f.(g.(&1))}
  end
end
