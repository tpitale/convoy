defmodule Convoy.QueueBehaviour do
  @type stream_name :: binary() | atom()
  @type partition :: any()
  @type data_value :: map()
  @type limit_value :: integer()

  @callback put(stream_name, partition, data_value) :: any()
  @callback get(stream_name, limit_value) :: Convoy.Queue.Record.t()
end
