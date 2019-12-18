defmodule Convoy.Services.Kinesis do
  @behaviour Convoy.Service

  alias ExAws.Kinesis

  @impl true
  def put_records(stream_name, records) do
    Kinesis.put_records(stream_name, records)
    |> ExAws.request!()

    :telemetry.execute(
      [:convoy, :kinesis, :put_records, :success],
      %{count: length(records)},
      %{stream: stream_name}
    )
  end

  @impl true
  def get_records(iterator, opts \\ [])
  def get_records(nil, _opts), do: {[], nil}

  def get_records(iterator, opts) do
    Kinesis.get_records(iterator, opts)
    |> ExAws.request()
    |> case do
      {:ok, %{"Records" => records, "NextShardIterator" => next_iterator}} ->
        :telemetry.execute(
          [:convoy, :kinesis, :get_records, :success],
          %{count: length(records)},
          %{iterator: iterator, next_iterator: next_iterator}
        )

        {
          records |> decode_records(),
          next_iterator
        }

      unexpected ->
        :telemetry.execute(
          [:convoy, :kinesis, :get_records, :unexpected],
          %{},
          %{result: unexpected}
        )

        {[], nil}
    end
  end

  @impl true
  def get_iterator(stream_name, shard_id, shard_iterator_type) do
    Kinesis.get_shard_iterator(stream_name, shard_id, shard_iterator_type)
    |> ExAws.request()
    |> case do
      {:ok, %{"ShardIterator" => iterator}} -> iterator
      _ -> nil
    end
  end

  # TODO: get_shards directly

  @impl true
  def describe_stream(stream_name) do
    # TODO: handle multiple shards
    Kinesis.describe_stream(stream_name)
    |> ExAws.request!()
  end

  defp decode_records(records) do
    records
    |> Enum.map(fn %{"Data" => data, "PartitionKey" => key} ->
      %{partition_key: key, data: Base.decode64!(data)}
    end)
  end
end
