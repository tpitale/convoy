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
