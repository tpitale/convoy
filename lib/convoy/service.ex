defmodule Convoy.Service do
  @type record :: %{data: binary(), partition_key: binary()}
  @callback put_records(stream_name :: binary(), records :: [record]) :: any
  @callback get_records(shard_iterator :: binary(), opts :: []) :: [any]
  @callback get_iterator(
              stream_name :: binary(),
              shard_id :: binary(),
              shard_iterator_type :: atom()
            ) :: any
end
