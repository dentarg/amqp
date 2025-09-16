require "bundler/inline"

# install and require these gems
gemfile do
  source "https://rubygems.org"
  gem "bunny", "2.24.0"
  gem "json"
end

# 1a) Build your first microservice:
# Develop a microservice that connects to LavinMQ, subscribes to "<group_name>_booking_requests" messages, extracts
# the "from" city, computes route distance between the city and the destination, creates a new message with the
# route distance, and publishes to the "<group_name>_bookings" queue. The distance computation does not need to be
# realistic (random is fine!).
#{ }"vilnius" and "kaunas" are the supported "from" cities.
#
# Use the button "Book Taxi" to generate the input message.
#
# Input (<group_name>_booking_requests queue):
# {"from": "vilnius", "to": "stockholm", "group_name": "your_group_name"}
# Output (<group_name>_bookings queue):
# {"from": "vilnius", "to": "stockholm", "group_name": "your_group_name", "distance": "5000"}

group_name = ARGV.shift || "patrik"
queue_name = "#{group_name}_booking_requests"
consume_timeout = Integer(ARGV.shift || 15)

opts = {
  verify_peer: true,
  tls_silence_warnings: true, # silence "Using TLS but no client certificate is provided"
}
amqp_url = ENV.fetch("AMQP_URL")
connection = Bunny.new(amqp_url, opts)

connection.start
puts "Connected"
channel = connection.create_channel
puts "Channel created"
queue = channel.queue(queue_name, durable: true)

mutex = Mutex.new
resource = ConditionVariable.new

puts "Waiting up to #{consume_timeout} seconds for booking request message"
messages = []
queue.subscribe(block: false) do |_delivery_info, _properties, body|
  puts "Received booking request: #{body}"
  messages << body
  mutex.synchronize { resource.signal } # signal that we consumed the message
end

mutex.synchronize { resource.wait(mutex, consume_timeout) }

exchange = channel.exchange("") # Declare a default direct exchange which is bound to all queues
messages.each do |message|
  booking_request = JSON.parse(message)
  from = booking_request.fetch("from")
  to = booking_request.fetch("to")
  booking = { from:, to:, group_name:, distance: rand(31337) }

  exchange.publish(booking.to_json, key: "#{group_name}_bookings")

  puts "Published booking: #{booking}"
end

connection.close
puts "Connection closed"
