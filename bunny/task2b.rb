require "bundler/inline"

# install and require these gems
gemfile do
  source "https://rubygems.org"
  gem "bunny", "2.24.0"
  gem "json"
  gem "excon"
end

Signal.trap("INT") do
  exit
end

group_name = "patrik"

# 2b) Share information (topic exchange):
# Create a topic exchange, bind it to taxi queues using city-based bindings (for instance bind one taxi to the city
#  (topic) vilnius and another to kaunas). Consume messages from "notifications"; if status is "alert", publish to
# the topic exchange using the city as the routing key.
# Verify in the management UI that the correct queue received the message.

# Input ("<group_name>_notifications" queue):
# {"city": "vilnius", "message": "Find more customers...", "status": "alerts", "group_name": "your_group_name"}
# Output (topic exchange "<group_name>_alerts", routing_key=<city-name>):
# {"info": "Find more customers..."}

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
api_data = Excon.get("https://workshop.lavinmq.com/taxis").body
taxis = JSON.parse(api_data).fetch("taxis").flat_map { |city, cars| cars.map { |car| Taxi.new(city, car) } }

opts = {
  verify_peer: true,
  tls_silence_warnings: true, # silence "Using TLS but no client certificate is provided"
}
amqp_url = ENV.fetch("AMQP_URL")
connection = Bunny.new(amqp_url, opts)

connection.start
puts "Connected"
channel = connection.create_channel

city_exchange = channel.topic("#{group_name}_alerts")
notifications_queue = channel.queue("#{group_name}_notifications", durable: true)

taxis.each do |taxi|
  queue_name = "#{group_name}_#{taxi.city}_#{taxi.car}"
  queue = channel.queue(queue_name, durable: true)

  queue.bind(city_exchange, routing_key: taxi.city)
  puts "Channel created: #{queue_name}"
end

puts "Waiting for notifications"
notifications_queue.subscribe(block: true) do |_delivery_info, _properties, body|
  puts "Received notification: #{body.inspect}"
  notification = Notification.new(body)

  if notification.alert?
    city_exchange.publish(notification.alert, routing_key: notification.city)
    puts "Alert published: #{notification.alert}"
  end
end
