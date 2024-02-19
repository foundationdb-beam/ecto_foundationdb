defmodule Ecto.Integration.TimeSeriesTest do
  use Ecto.Integration.Case, async: true

  alias Ecto.Adapters.FoundationDB
  alias Ecto.Integration.TestRepo

  alias EctoFoundationDB.Schemas.Event

  import Ecto.Query

  @moduletag :integration
  describe "timeseries index" do
    test "timeseries consistency", context do
      tenant = context[:tenant]

      # Insert
      {:ok, event = %Event{id: event_id}} =
        %Event{timestamp: ~N[2070-01-01 00:00:00.000000]}
        |> FoundationDB.usetenant(tenant)
        |> TestRepo.insert()

      {:ok, _} =
        %Event{timestamp: ~N[2777-01-01 00:00:00.000000]}
        |> FoundationDB.usetenant(tenant)
        |> TestRepo.insert()

      # Because write_primary: false
      nil = TestRepo.get(Event, event.id, prefix: tenant)

      # All
      query =
        from(e in Event,
          where:
            e.timestamp > ^~N[1970-01-01 00:00:00] and
              e.timestamp < ^~N[2100-01-01 00:00:00]
        )

      assert [%Event{}] = TestRepo.all(query, prefix: tenant)

      # Update
      assert {1, _} = TestRepo.update_all(query, [set: [data: "foo"]], prefix: tenant)

      assert [%Event{id: ^event_id, data: "foo"}] = TestRepo.all(query, prefix: tenant)

      # Delete
      assert {1, _} = TestRepo.delete_all(query, prefix: tenant)

      assert [] == TestRepo.all(query, prefix: tenant)
    end
  end
end
