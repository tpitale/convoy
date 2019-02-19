defmodule Convoy.QueueTest do
  use ExUnit.Case

  import Mox

  alias Convoy.Queue.Record

  @stream_name "device_service_test"

  setup_all do
    Mox.defmock(Convoy.MockService, for: Convoy.Service)

    :ok
  end

  test "puts a record into the batch" do
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
end
