defmodule Convoy.Queue do
  use GenServer

  @behaviour Convoy.QueueBehaviour

  @type stream_identifier :: binary | atom

  defmodule State do
    @batch_timeout_ms 5000
    @default_service Convoy.Services.Kinesis

    def batch_timeout_ms(), do: @batch_timeout_ms
    def default_service(), do: @default_service

    defstruct stream: nil,
              stream_id: nil,
              service: @default_service,
              batch_timeout: @batch_timeout_ms,
              poll_interval: nil,
              shards: [],
              handlers: %{},
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

  def init(opts) when is_list(opts), do: opts |> Enum.into(%{}) |> init()

  def init(opts) when is_map(opts) do
    stream_id = stream_id(opts)
    stream = opts[:stream]
    service = opts[:service] || State.default_service()
    batch_timeout = opts[:batch_timeout] || State.batch_timeout_ms()

    batch_transmit_after(batch_timeout)

    {
      :ok,
      {
        :queue.new(),
        %State{
          service: service,
          stream_id: stream_id,
          stream: stream,
          shards: shards(stream, service),
          batch_timeout: batch_timeout,
          poll_interval: opts[:poll_interval],
          iterator_type: opts[:iterator_type] || :latest
        }
      },
      {:continue, :start_polling}
    }
  end

  @doc """
  Put a record into the stream or stream identifier with a given partition key
  and data value.
  """
  @spec put(stream_identifier(), binary(), map()) :: any
  def put(stream_id, partition_key, data) do
    record = %Record{
      partition_key: partition_key,
      data: data |> encode()
    }

    GenServer.cast(:"stream_#{stream_id}", {:put_record, record})
  end

  @doc """
  Get a list of records for a stream or stream identifier
  """
  @spec get(stream_identifier(), integer()) :: [Record.t()]
  def get(stream_id, limit \\ 10) do
    GenServer.call(:"stream_#{stream_id}", {:get_records, limit})
  end

  @doc """
  Transmit the outbound queue
  """
  @spec flush(stream_identifier()) :: any
  def flush(stream_id) do
    send(:"stream_#{stream_id}", :transmit)
  end

  @doc """
  Attach a handler function on the stream
  """
  @spec attach(stream_identifier(), binary(), function(), map()) :: any
  def attach(stream_id, handler_id, handler_fn, extra_opts) do
    GenServer.cast(:"stream_#{stream_id}", {:attach, handler_id, {handler_fn, extra_opts}})
  end

  @doc """
  Detach a handler function from the stream
  """
  @spec detach(stream_identifier(), binary()) :: any
  def detach(stream_id, handler_id) do
    GenServer.cast(:"stream_#{stream_id}", {:detach, handler_id})
  end

  def handle_continue(:start_polling, {_, opts} = state) do
    next_poll(opts.poll_interval)

    {:noreply, state}
  end

  def handle_cast({:put_record, record}, {queue, %{batch_timeout: 0} = opts}) do
    # TODO: what is a reasonable minimum batch timeout? 20ms? 50ms?
    transmit_to([record], opts.stream, with: opts.service)

    {:noreply, {queue, opts}}
  end

  def handle_cast({:put_record, record}, {queue, opts}) do
    {:noreply, {:queue.in(record, queue), opts}}
  end

  def handle_cast({:attach, handler_id, handler}, {queue, opts}) do
    new_handlers = Map.put(opts.handlers, handler_id, handler)

    :telemetry.execute(
      [:convoy, :queue, :attach],
      %{handler_count: map_size(new_handlers)},
      %{stream: opts.stream, handler_id: handler_id}
    )

    {:noreply, {queue, %{opts | handlers: new_handlers}}}
  end

  def handle_cast({:detach, handler_id}, {queue, opts}) do
    new_handlers = Map.delete(opts.handlers, handler_id)
    handler_count = map_size(new_handlers)

    :telemetry.execute(
      [:convoy, :queue, :detach],
      %{handler_count: handler_count},
      %{stream: opts.stream, handler_id: handler_id}
    )

    # reset iterators on all shards if there are no handlers left
    shards =
      case handler_count do
        0 -> reset_iterators(opts.shards)
        _ -> opts.shards
      end

    {:noreply, {queue, %{opts | handlers: new_handlers, shards: shards}}}
  end

  def handle_info(:transmit, {queue, opts}) do
    transmit_to(queue, opts.stream, with: opts.service)

    batch_transmit_after(opts.batch_timeout)

    {:noreply, {:queue.new(), opts}}
  end

  # Do not get_records when there are no handlers
  def handle_info(:poll, {_, %{handlers: handlers} = opts} = state) when handlers == %{} do
    next_poll(opts.poll_interval)

    {:noreply, state}
  end

  def handle_info(:poll, {_, opts} = state) do
    # TODO: tunable limit option
    {_records, new_state} = get_records(10, state)

    next_poll(opts.poll_interval)

    {:noreply, new_state}
  end

  def handle_call(:current_queue, _from, {queue, opts}) do
    {:reply, queue |> :queue.to_list(), {queue, opts}}
  end

  def handle_call({:get_records, limit}, _from, state) do
    {records, new_state} = get_records(limit, state)

    {:reply, records, new_state}
  end

  defp get_records(limit, {queue, opts}) do
    {shard, shards} = rotate_out(opts.shards)

    {records, next_iterator} =
      case shard do
        %{iterator: nil} ->
          opts.service.get_iterator(opts.stream, shard.id, opts.iterator_type)

        %{iterator: iterator} ->
          iterator
      end
      |> opts.service.get_records(limit: limit)

    :telemetry.execute(
      [:convoy, :queue, :records_received],
      %{count: length(records)},
      %{stream: opts.stream, shard_id: shard.id, next_iterator: next_iterator}
    )

    notify_handlers(records, opts.handlers, opts.stream_id)

    {
      records,
      {queue, %{opts | shards: rotate_in(shards, %{shard | iterator: next_iterator})}}
    }
  end

  # Empty queue is a no-op
  defp transmit_to([], _stream, with: _service), do: nil

  defp transmit_to(records, stream, with: service) when is_list(records) do
    :telemetry.execute(
      [:convoy, :queue, :records_sent],
      %{count: length(records)},
      %{stream: stream}
    )

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

  defp reset_iterators(shards), do: shards |> Enum.map(&%{&1 | iterator: nil})

  defp next_poll(nil), do: nil
  defp next_poll(interval) when interval <= 0, do: nil
  defp next_poll(interval), do: Process.send_after(self(), :poll, interval)

  defp notify_handlers(records, handlers, stream_id) do
    Enum.each(handlers, &notify_handler(records, &1, stream_id))
  end

  defp notify_handler(records, {handler_id, {handle_fn, info}}, stream_id) do
    try do
      Enum.each(records, &handle_fn.(&1, info))
    rescue
      # TODO: emit logs?
      _e -> detach(stream_id, handler_id)
    end
  end
end
