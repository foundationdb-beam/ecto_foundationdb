defmodule FDBBarrierStreamTest do
  use ExUnit.Case, async: true

  alias FDB.BarrierStream

  test "new(enumerable)" do
    assert %BarrierStream{} = BarrierStream.new(1..10)
  end

  test "advance(bs, fun)" do
    bs = BarrierStream.new(1..10)

    assert {10, %BarrierStream{enum: [1, 2 | _], funs: []}} = BarrierStream.advance(bs, &length/1)
  end

  test "to_stream(bs)" do
    bs =
      BarrierStream.new(1..10)
      |> BarrierStream.then(&Stream.map(&1, fn x -> x * 100 end))

    assert [100, 200 | _] = BarrierStream.to_stream(bs) |> Enum.to_list()
  end

  test "then(bs, apply)" do
    bs =
      BarrierStream.new(1..10)
      |> BarrierStream.then(&Stream.map(&1, fn x -> x * 100 end))

    %BarrierStream{enum: stream, funs: funs} = bs

    refute is_list(stream)
    assert [_] = funs
    assert [1, 2 | _] = Enum.to_list(stream)
    assert [100, 200 | _] = BarrierStream.to_stream(bs) |> Enum.to_list()
  end

  test "set_barrier(bs)" do
    bs =
      BarrierStream.new(1..10)
      |> BarrierStream.then(&Stream.map(&1, fn x -> x * 100 end))
      |> BarrierStream.set_barrier()

    assert {10, %BarrierStream{enum: [100, 200 | _], funs: []}} =
             BarrierStream.advance(bs, &length/1)
  end
end
