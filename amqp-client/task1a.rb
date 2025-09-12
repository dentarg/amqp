require "bundler/inline"

# install and require these gems
gemfile do
  source "https://rubygems.org"
  gem "amqp-client", "1.1.7"
  gem "json"
end

group_name = "patrik"

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

queue_name = "#{group_name}_booking_requests"
consume_timeout = Integer(ARGV.first || 15)

amqp_url = ENV.fetch("AMQP_URL")
client = AMQP::Client.new(amqp_url)
client.start
puts "Connected"

request_queue = client.queue(queue_name)
request_queue.bind("amq.topic", queue_name)

mutex = Mutex.new
resource = ConditionVariable.new

puts "Waiting for booking request message"
messages = []
request_queue.subscribe do |msg|
  puts "Received booking request: #{msg.body}"
  messages << msg.body
  msg.ack
  mutex.synchronize { resource.signal } # signal that we consumed the message
end

mutex.synchronize { resource.wait(mutex, consume_timeout) }

bookings_queue = client.queue("#{group_name}_bookings")
messages.each do |message|
  booking_request = JSON.parse(message)
  from = booking_request.fetch("from")
  to = booking_request.fetch("to")
  booking = { from:, to:, group_name:, distance: rand(31337) }

  bookings_queue.publish(booking.to_json)

  puts "Published booking: #{booking}"
end

client.stop
puts "Connection closed"
