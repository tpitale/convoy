defmodule Convoy.Queue do
  use GenServer

  @batch_timeout_ms 5000
  @default_service ExAws.Kinesis

  defmodule Record do
    defstruct data: "{}", partition_key: nil
  end

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: :"stream_#{opts[:stream]}")
  end

  def init(opts) do
    batch_timeout = opts[:batch_timeout] || @batch_timeout_ms

    batch_transmit_after(batch_timeout)

    {:ok,
     {
       :queue.new(),
       %{
         service: opts[:service] || @default_service,
         stream: opts[:stream],
         batch_timeout: batch_timeout
       }
     }}
  end

  def put(stream, partition_key, data) do
    record = %Record{
      partition_key: partition_key,
      data: data |> Poison.encode!()
    }

    GenServer.cast(:"stream_#{stream}", {:put_record, record})
  end

  def handle_cast({:put_record, record}, {queue, opts}) do
    {:noreply, {:queue.in(record, queue), opts}}
  end

  def handle_info(:transmit, {queue, opts}) do
    # TODO: limit how many we take from the queue in a batch?
    queue
    |> :queue.to_list()
    |> transmit_to(opts[:stream], with: opts[:service])

    batch_transmit_after(opts[:batch_timeout])

    {:noreply, {:queue.new(), opts}}
  end

  def handle_call(:current_queue, _from, {queue, opts}) do
    {:reply, queue |> :queue.to_list(), {queue, opts}}
  end

  defp transmit_to(records, stream, with: service) do
    service.put_records(stream, records)
  end

  defp batch_transmit_after(timeout) when timeout > 0 do
    Process.send_after(self(), :transmit, timeout)
  end

  # If invalid timeout, manual trigger of batch transmission
  defp batch_transmit_after(_timeout), do: nil
end
