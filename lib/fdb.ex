defmodule FDB do
  @moduledoc false

  def stream_range(tx, start_key, end_key, options \\ []) do
    FDB.Stream.range(tx, start_key, end_key, options)
  end
end
