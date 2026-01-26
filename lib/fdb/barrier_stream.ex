defmodule FDB.BarrierStream do
  @moduledoc false

  # Defines a Stream that has a single barrier somewhere in its execution.
  # THe stream may be advanced to this barrier when appropriate due to some
  # external requirement.
  defstruct [:enum, :funs]

  def new(enumerable) do
    %__MODULE__{enum: enumerable, funs: []}
  end

  def advance(bs = %__MODULE__{enum: enumerable}, fun \\ &Function.identity/1) do
    enum = Enum.to_list(enumerable)
    {fun.(enum), %{bs | enum: enum}}
  end

  def set_barrier(bs = %__MODULE__{}) do
    %{bs | enum: to_stream(bs), funs: []}
  end

  def then(bs = %__MODULE__{funs: funs}, apply), do: %{bs | funs: [apply | funs]}

  def to_stream(bs = %__MODULE__{}) do
    %{enum: enum, funs: funs} = bs

    funs
    |> Enum.reverse()
    |> Enum.reduce(enum, fn fun, enum0 ->
      fun.(enum0)
    end)
  end
end
