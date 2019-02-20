use Mix.Config

config :ex_aws,
  debug_requests: true,
  kinesis: %{
    host: "localhost",
    port: 4567,
    scheme: "http://",
    access_key_id: [{:system, "KINESIS_AWS_ACCESS_KEY_ID"}, :instance_role],
    secret_access_key: [{:system, "KINESIS_SECRET_ACCESS_KEY"}, :instance_role]
  }
