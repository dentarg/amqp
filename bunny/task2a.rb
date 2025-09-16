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

# 2a) Information handler (fanout exchange):
# Create an information-sharing app. Retrieve all taxi cars via API (https://workshop.lavinmq.com/taxis).
# Create a queue per taxi (suggested naming pattern: "<group_name>_<city>_<taxi>") develop a microservice creating
# a fanout exchange, and bind it to all taxi queues. Consume messages from the "<group_name>_notifications" queue;
# if status is "news", publish the message to the fanout exchange.
# Verify in the management UI that the taxi queues received the expected messages.
#
# Input ("<group_name>_notifications" queue):
# {"city": "vilnius", "message": "free fika...", "status": "news", "group_name": "your_group_name"}
# Output (fanout exchange "<group_name>_news"):
# {"info": "free fika..."}

group_name = ARGV.shift || "patrik"

Taxi = Data.define(:city, :car)
Notification = Struct.new(:payload) do
  def initialize(payload)
    @data = JSON.parse(payload)
  end
  def city = @data.fetch("city")
  def message = @data.fetch("message")
  def status = @data.fetch("status")
  def news? = status == "news"
  def news = { info: message }.to_json
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

taxi_queues = taxis.map do |taxi|
  queue_name = "#{group_name}_#{taxi.city}_#{taxi.car}"
  queue = channel.queue(queue_name, durable: true)
  puts "Channel created: #{queue_name}"
  queue
end

news_exchange = channel.fanout("taxi.news")
taxi_queues.each { |queue| queue.bind(news_exchange) }

notifications_queue = channel.queue("#{group_name}_notifications", durable: true)

puts "Waiting for notifications"
notifications_queue.subscribe(block: true) do |_delivery_info, _properties, body|
  puts "Received notification: #{body.inspect}"

  notification = Notification.new(body)

  if notification.news?
    news_exchange.publish(notification.news)
    puts "News alert published: #{notification.news}"
  end
end
