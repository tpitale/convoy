# Convoy

Convoy wraps and adapts different queue/stream systems, starting with AWS Kinesis.

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `convoy` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:convoy, "~> 0.1.0"}
  ]
end
```

## Starting a Queue ##

Any `Queue` should be supervised, so add it to your supervision tree. Or, if you
need to start one after startup, you can make use of `DynamicSupervisor`.

```ex
{Convoy.Queue, [stream: :your_stream_name]}
```

## Queue Configuration ##

The only required option is `stream`. The `stream` option will be used as the
`stream_id`, if none is provided.

If you wish to connect to the same stream multiple times, you have to pass
a `stream_id`. It is not possible to use the same `stream` as the identifier.

```ex
# Defaults
%{
  stream: nil,
  stream_id: nil,
  service: Convoy.Services.Kinesis,
  batch_timeout: 5000,
  poll_interval: nil,
  iterator_type: :latest
}
```

### Batch Timeout ###

If you wish to disable batching, or increase the frequency of batches transmitting
to the service, you can change the `batch_timeout` option from the default of 5
seconds. This value is configured in `ms`, so 5000 is 5 seconds.

Setting this value to `0` will disable batching.

### Poll Interval ###

Disabled by default, setting the `poll_interval` value will configure the `Queue`
to poll for new records at that interval. You'll want to combine this with calls
to `attach_handler/4`. Polling will begin only after there is at least one handler
attached for the `stream_id`. If all handlers are detached, polling will stop.

### Service ###

At this time, Convoy only supports Kinesis, but you can write your own `service`
that fulfills the `Convoy.Service` behaviour.

**TODO:** Planned support for services like SQS and Kafka.

### Kinesis Configuration ###

If using Kinesis, configure the underlying `ex_aws_kinesis` library in your `dev.exs`:

```
config :ex_aws,
  debug_requests: true,
  kinesis: %{
    host: "localhost",
    port: 4567,
    scheme: "http://",
    access_key_id: [{:system, "KINESIS_AWS_ACCESS_KEY_ID"}, :instance_role],
    secret_access_key: [{:system, "KINESIS_SECRET_ACCESS_KEY"}, :instance_role]
  }
```

### Local Development ###

We recommend running Kinesalite locally for development.





```
{:ok, response} = ExAws.Kinesis.describe_stream("device_service_dev") |> ExAws.request
shard_id = response["StreamDescription"]["Shards"] |> hd() |> Map.get("ShardId")

{:ok, iterator} = ExAws.Kinesis.get_shard_iterator("device_service_dev", shard_id, :latest) |> ExAws.request

# get the shard id and iterator at gen_server/gen_stage init

{:ok, response} = ExAws.Kinesis.get_records(iterator["ShardIterator"], %{limit: 10}) |> ExAws.request

# includes the next shard iterator … store in database in case we crash

# route the work to the next gen_stage based on the partition key

# is there any way to filter records on partition key?

Convoy.Queue.put("device_service_dev", "devices", "{}")
```
