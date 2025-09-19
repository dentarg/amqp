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

group_name = ARGV.first || "patrik"

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
api_url = ENV.fetch("TAXIS_API_URL", "https://workshop.lavinmq.com/taxis")
api_data = Excon.get(api_url).body
taxis = JSON.parse(api_data).fetch("taxis").flat_map { |city, cars| cars.map { |car| Taxi.new(city, car) } }

amqp_url = ENV.fetch("AMQP_URL")
client = AMQP::Client.new(amqp_url)
client.start
puts "Connected"

taxi_queues = taxis.map do |taxi|
  queue_name = "#{group_name}_#{taxi.city}_#{taxi.car}"
  queue = client.queue(queue_name) # Declares a durable queue
  puts "Queue created: #{queue_name}"
  queue
end

news_exchange = client.fanout("taxi.news2")
taxi_queues.each { |queue| queue.bind(news_exchange, _binding_key = "") }

notifications_queue = client.queue("notifications") # Declares a durable queue

puts "Waiting for notifications"
messages = []
notifications_queue.subscribe do |msg|
  puts "Received notification: #{msg.body}"
  msg.ack

  notification = Notification.new(msg.body)

  if notification.news?
    news_exchange.publish(notification.news)
    puts "News alert published: #{notification.news}"
  end
end

sleep # don't exit
