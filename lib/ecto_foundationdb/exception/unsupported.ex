defmodule EctoFoundationDB.Exception.Unsupported do
  @moduledoc """
  This exception is raised when the application uses an Ecto feature that is not
  supported by the FoundationDB Adapter.
  """
  defexception [:message]
end
