defmodule EctoFoundationDB.Layer.DecodedKV do
  @moduledoc false
  @enforce_keys [:codec, :data_object, :multikey?, :range]
  defstruct @enforce_keys
end
