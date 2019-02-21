defmodule Convoy.SubscriptionProducer do
  use GenStage

  import Logger, only: [info: 1]

  @default_polling_frequency 5000

  def start_link(opts) do
    GenStage.start_link(__MODULE__, opts, name: :"subscription_producer_#{opts[:stream]}")
  end

  def init(opts) do
    {:producer,
     {0,
      %{
        stream: opts[:stream],
        polling_frequency: opts[:polling_frequency] || @default_polling_frequency
      }}}
  end

  def handle_demand(incoming_demand, {current_demand, opts}) do
    info("Incoming subscription demand: #{incoming_demand}")
    dispatch_records({incoming_demand + current_demand, opts})
  end

  def handle_info(:dispatch_records, state), do: dispatch_records(state)

  defp dispatch_records({0, opts}), do: {:noreply, [], {0, opts}}

  defp dispatch_records({demand, opts}) do
    records = Convoy.Queue.get(opts[:stream], demand)

    remaining_demand = max(demand - (records |> length()), 0)

    info("Remaining demand: #{remaining_demand}")

    Process.send_after(self(), :dispatch_records, opts[:polling_frequency])

    {:noreply, records, {remaining_demand, opts}}
  end
end
