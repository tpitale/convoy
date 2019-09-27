defmodule Convoy.Queue do
  use GenServer

  @behaviour Convoy.QueueBehaviour

  @batch_timeout_ms 5000
  @default_service Convoy.Services.Kinesis

  defmodule State do
    defstruct stream: nil,
              stream_id: nil,
              service: nil,
              batch_timeout: nil,
              shards: [],
              iterator_type: :latest
  end

  defmodule Record do
    @type t :: %__MODULE__{data: binary(), partition_key: binary()}

    defstruct data: "{}", partition_key: nil
  end

  defmodule Shard do
    defstruct id: nil, iterator: nil
  end

  def stream_id(%{stream_id: stream_id}), do: stream_id
  def stream_id(%{stream: stream}), do: stream
  def stream_id(%{stream_id: nil, stream: stream}), do: stream

  def child_spec(opts) do
    %{
      id: :"stream_#{stream_id(opts)}",
      start: {__MODULE__, :start_link, [opts]},
      restart: :transient,
      type: :worker
    }
  end

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: :"stream_#{stream_id(opts)}")
  end

  def init(opts) do
    stream_id = stream_id(opts)
    stream = opts[:stream]
    service = opts[:service] || @default_service
    batch_timeout = opts[:batch_timeout] || @batch_timeout_ms

    batch_transmit_after(batch_timeout)

    {:ok,
     {
       :queue.new(),
       %State{
         service: service,
         stream_id: stream_id,
         stream: stream,
         shards: shards(stream, service),
         batch_timeout: batch_timeout,
         iterator_type: opts[:iterator_type] || :latest
       }
     }}
  end

  def put(stream_id, partition_key, data) do
    record = %Record{
      partition_key: partition_key,
      data: data |> encode()
    }

    GenServer.cast(:"stream_#{stream_id}", {:put_record, record})
  end

  def get(stream_id, limit \\ 10) do
    GenServer.call(:"stream_#{stream_id}", {:get_records, limit})
  end

  @doc """
  Transmit the outbound queue
  """
  def flush(stream_id) do
    send(:"stream_#{stream_id}", :transmit)
  end

  def handle_cast({:put_record, record}, {queue, %{batch_timeout: 0} = opts}) do
    # TODO: what is a reasonable minimum batch timeout? 20ms? 50ms?
    transmit_to([record], opts.stream, with: opts.service)

    {:noreply, {queue, opts}}
  end

  def handle_cast({:put_record, record}, {queue, opts}) do
    {:noreply, {:queue.in(record, queue), opts}}
  end

  # TODO: polling time
  def handle_cast({:subscribe, match_fn, call_fn}, {queue, opts}) do
    {:noreply, {queue, %{opts | subscribers: [{match_fn, call_fn}, opts.subscribers]}}}
  end

  def handle_info(:transmit, {queue, opts}) do
    transmit_to(queue, opts.stream, with: opts.service)

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

  defp transmit_to(records, stream, with: service) when is_list(records) do
    service.put_records(stream, records)
  end

  defp transmit_to(queue, stream, with: service) do
    queue
    |> :queue.to_list()
    |> transmit_to(stream, with: service)
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
