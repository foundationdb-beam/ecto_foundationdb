defmodule EctoFoundationDB.Exception.IncorrectTenancy do
  @moduledoc """
  This exception is raised when there is a conflict in the tenant used.
  """
  defexception [:message]
end
