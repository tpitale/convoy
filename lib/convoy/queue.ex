defmodule Convoy.Queue do
  use GenServer

  @batch_timeout_ms 5000
  @default_service Convoy.Services.Kinesis

  defmodule State do
    defstruct stream: nil, service: nil, batch_timeout: nil, shards: [], iterator_type: :latest
  end

  defmodule Record do
    defstruct data: "{}", partition_key: nil
  end

  defmodule Shard do
    defstruct id: nil, iterator: nil
  end

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: :"stream_#{opts[:stream]}")
  end

  def init(opts) do
    stream = opts[:stream]
    service = opts[:service] || @default_service
    batch_timeout = opts[:batch_timeout] || @batch_timeout_ms

    batch_transmit_after(batch_timeout)

    {:ok,
     {
       :queue.new(),
       %State{
         service: service,
         stream: stream,
         shards: shards(stream, service),
         batch_timeout: batch_timeout,
         iterator_type: opts[:iterator_type] || :latest
       }
     }}
  end

  def put(stream, partition_key, data) do
    record = %Record{
      partition_key: partition_key,
      data: data |> encode()
    }

    GenServer.cast(:"stream_#{stream}", {:put_record, record})
  end

  def get(stream, limit \\ 10) do
    GenServer.call(:"stream_#{stream}", {:get_records, limit})
  end

  def handle_cast({:put_record, record}, {queue, opts}) do
    {:noreply, {:queue.in(record, queue), opts}}
  end

  def handle_info(:transmit, {queue, opts}) do
    # TODO: limit how many we take from the queue in a batch?
    queue
    |> :queue.to_list()
    |> transmit_to(opts.stream, with: opts.service)

    batch_transmit_after(opts.batch_timeout)

    {:noreply, {:queue.new(), opts}}
  end

  def handle_call(:current_queue, _from, {queue, opts}) do
    {:reply, queue |> :queue.to_list(), {queue, opts}}
  end

  def handle_call({:get_records, limit}, _from, {queue, opts}) do
    {shard, shards} = rotate_out(opts.shards)

    {records, next_iterator} =
      case shard do
        %{iterator: nil} ->
          opts.service.get_iterator(opts.stream, shard.id, opts.iterator_type)

        %{iterator: iterator} ->
          iterator
      end
      |> opts.service.get_records(limit: limit)

    {:reply, records,
     {queue, %{opts | shards: rotate_in(shards, %{shard | iterator: next_iterator})}}}
  end

  # Empty queue is a no-op
  defp transmit_to([], _stream, with: _service), do: nil

  defp transmit_to(records, stream, with: service) do
    service.put_records(stream, records)
  end

  defp batch_transmit_after(timeout) when timeout > 0 do
    Process.send_after(self(), :transmit, timeout)
  end

  # If invalid timeout, manual trigger of batch transmission
  defp batch_transmit_after(_timeout), do: nil

  defp encode(data) when is_binary(data), do: data
  defp encode(data), do: data |> Poison.encode!()

  defp shards(stream, service) do
    service.describe_stream(stream)
    |> case do
      %{"StreamDescription" => %{"Shards" => shards}} ->
        shards |> Enum.map(fn %{"ShardId" => id} -> %Shard{id: id} end)
    end
  end

  defp rotate_out([shard | shards]), do: {shard, shards}
  defp rotate_in(shards, shard), do: shards ++ [shard]
end
