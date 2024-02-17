defmodule Ecto.Integration.TimeSeriesTest do
  use Ecto.Integration.Case

  alias Ecto.Integration.TestRepo
  alias Ecto.Adapters.FoundationDB
  alias EctoFoundationDB.Schemas.Event

  import Ecto.Query

  @moduletag :integration
  describe "timeseries index" do
    test "timeseries consistency", context do
      tenant = context[:tenant]

      {:ok, event} =
        %Event{timestamp: NaiveDateTime.utc_now()}
        |> FoundationDB.usetenant(tenant)
        |> TestRepo.insert()

      # Because write_primary: false
      nil = TestRepo.get(Event, event.id, prefix: tenant)

      assert [%Event{}] =
               from(e in Event,
                 where:
                   e.timestamp > ^~N[1970-01-01 00:00:00] and
                     e.timestamp < ^~N[2999-01-01 00:00:00]
               )
               |> TestRepo.all(prefix: tenant)

      # TODO - updates, deletes
    end
  end
end
