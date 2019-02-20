defmodule Convoy.QueueTest do
  use ExUnit.Case, async: false

  import Mox

  alias Convoy.Queue.Record

  @stream_name "device_service_test"

  setup_all do
    Mox.defmock(Convoy.MockService, for: Convoy.Service)

    :ok
  end

  test "puts a record into the batch" do
    set_mox_global()

    Convoy.MockService
    |> stub(:describe_stream, fn _stream_name ->
      %{"StreamDescription" => %{"Shards" => []}}
    end)

    {:ok, _pid} =
      Convoy.Queue.start_link(%{
        stream: @stream_name,
        batch_timeout: 0,
        service: Convoy.MockService
      })

    records = [%Record{partition_key: "devices", data: "{\"id\":2000012345}"}]

    Convoy.Queue.put(@stream_name, "devices", %{id: 2_000_012_345})

    assert records == GenServer.call(:"stream_#{@stream_name}", :current_queue)
  end

  test "transmits a batch of records to service" do
    record = %Record{
      data: %{id: 2_000_012_345} |> Poison.encode!(),
      partition_key: "devices"
    }

    queue = :queue.in(record, :queue.new())

    state =
      {queue,
       %{
         service: Convoy.MockService,
         stream: @stream_name,
         batch_timeout: 0
       }}

    parent_id = self()

    service_mock =
      Convoy.MockService
      |> expect(:put_records, 1, fn stream_name, records ->
        send(parent_id, {:put_records, [stream_name, records]})
      end)

    Convoy.Queue.handle_info(:transmit, state)

    verify!(service_mock)

    assert_received({
      :put_records,
      [
        "device_service_test",
        [%Convoy.Queue.Record{partition_key: "devices", data: "{\"id\":2000012345}"}]
      ]
    })
  end

  test "retrieves shards for a stream on init" do
    set_mox_global()

    parent_id = self()

    service_mock =
      Convoy.MockService
      |> expect(:describe_stream, 1, fn stream_name ->
        send(parent_id, {:describe_stream, stream_name})
        %{"StreamDescription" => %{"Shards" => []}}
      end)

    {:ok, _pid} =
      Convoy.Queue.start_link(%{
        stream: @stream_name,
        batch_timeout: 0,
        service: Convoy.MockService
      })

    verify!(service_mock)

    assert_received({:describe_stream, "device_service_test"})
  end

  test "gets iterator and records when shard has no iterator; stores next iterator in shard" do
    parent_id = self()

    state =
      {:queue.new(),
       %Convoy.Queue.State{
         service: Convoy.MockService,
         stream: @stream_name,
         batch_timeout: 0,
         shards: [%Convoy.Queue.Shard{id: "shard1-00001", iterator: nil}]
       }}

    service_mock =
      Convoy.MockService
      |> expect(:get_iterator, 1, fn stream_name, shard_id, iterator_type ->
        send(parent_id, {:get_iterator, stream_name, shard_id, iterator_type})
        "random-iterator-value"
      end)
      |> expect(:get_records, 1, fn iterator, _opts ->
        send(parent_id, {:get_records, iterator})
        {["some json", "some more json"], "random-iterator-value-2"}
      end)

    {:reply, records, {_queue, new_state}} =
      Convoy.Queue.handle_call({:get_records, 10}, self(), state)

    [%{iterator: next_iterator}] = new_state.shards

    assert ["some json", "some more json"] == records
    assert "random-iterator-value-2" == next_iterator

    verify!(service_mock)

    assert_received({:get_iterator, "device_service_test", "shard1-00001", :latest})
    assert_received({:get_records, "random-iterator-value"})
  end

  test "gets records when shard has iterator" do
    parent_id = self()

    state =
      {:queue.new(),
       %Convoy.Queue.State{
         service: Convoy.MockService,
         stream: @stream_name,
         batch_timeout: 0,
         shards: [%Convoy.Queue.Shard{id: "shard1-00001", iterator: "random-iterator-value"}]
       }}

    service_mock =
      Convoy.MockService
      |> expect(:get_iterator, 0, fn _stream_name, _shard_id, _iterator_type ->
        nil
      end)
      |> expect(:get_records, 1, fn iterator, _opts ->
        send(parent_id, {:get_records, iterator})
        {[], "random-iterator-value-2"}
      end)

    {:reply, _records, {_queue, new_state}} =
      Convoy.Queue.handle_call({:get_records, 10}, self(), state)

    [%{iterator: next_iterator}] = new_state.shards

    assert "random-iterator-value-2" == next_iterator

    verify!(service_mock)

    assert_received({:get_records, "random-iterator-value"})
  end
end
