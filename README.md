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

### Configuration ###

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
