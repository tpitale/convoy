defmodule Convoy.Service do
  @type iterator :: binary()
  @type record :: %{data: binary(), partition_key: binary()}
  @callback put_records(stream_name :: binary(), records :: [record]) :: any
  @callback get_records(shard_iterator :: iterator, opts :: []) :: {[map()], iterator}
  @callback describe_stream(stream_name :: binary()) :: map()
  @callback get_iterator(
              stream_name :: binary(),
              shard_id :: binary(),
              shard_iterator_type :: atom()
            ) :: iterator
end
