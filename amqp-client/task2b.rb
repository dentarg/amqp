require "bundler/inline"

# install and require these gems
gemfile do
  source "https://rubygems.org"
  gem "amqp-client", "1.2.1"
  gem "excon"
  gem "json"
end

Signal.trap("INT") do
  exit
end

# 2b) Share information (topic exchange):
# Create a topic exchange, bind it to taxi queues using city-based bindings (for instance bind one taxi to the city
#  (topic) vilnius and another to kaunas). Consume messages from "notifications"; if status is "alert", publish to
# the topic exchange using the city as the routing key.
# Verify in the management UI that the correct queue received the message.

# Input ("<group_name>_notifications" queue):
# {"city": "vilnius", "message": "Find more customers...", "status": "alert", "group_name": "your_group_name"}
# Output (topic exchange "<group_name>_alerts", routing_key=<city-name>):
# {"info": "Find more customers..."}

group_name = ARGV.first || "patrik"
consume_timeout = Integer(ARGV.first || 15)

Taxi = Data.define(:city, :car)
Notification = Struct.new(:payload) do
  def initialize(payload)
    @data = JSON.parse(payload)
  end
  def city = @data.fetch("city")
  def message = @data.fetch("message")
  def status = @data.fetch("status")
  def alert? = status == "alert"
  def alert = { info: message }.to_json
end
api_url = ENV.fetch("TAXIS_API_URL", "https://workshop.lavinmq.com/taxis")
api_data = Excon.get(api_url).body
taxis = JSON.parse(api_data).fetch("taxis").flat_map { |city, cars| cars.map { |car| Taxi.new(city, car) } }

amqp_url = ENV.fetch("AMQP_URL")
client = AMQP::Client.new(amqp_url)
client.start
puts "Connected"

city_exchange = client.topic("alerts2")

taxis.each do |taxi|
  queue_name = "#{taxi.city}_#{taxi.car}"
  queue = client.queue(queue_name)

  queue.bind(city_exchange, _binding_key = taxi.city)

  puts "Queue created: #{queue_name} bound to exchange '#{city_exchange.name}'"
end

puts "Waiting for notifications"
client.queue("notifications").subscribe do |msg|
  puts "Received notification: #{msg.body.inspect}"
  notification = Notification.new(msg.body)
  msg.ack

  if notification.alert?
    city_exchange.publish(notification.alert, _routing_key = notification.city)
    puts "Alert published: #{notification.alert}"
  end
end

sleep
