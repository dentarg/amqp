# lib/tasks/benchmark.rake
namespace :benchmarks do
  desc "Benchmark Bunny vs AMQP-client for Task 1a"
  task :amqp_clients do
    require "benchmark/ips"
    require "json"
    require "bunny"
    require "amqp/client"
    require "optparse"
    require "uri"
    require "timeout"

    CITIES = [ "vilnius", "kaunas", "stockholm", "london", "paris", "berlin", "madrid", "rome", "warsaw", "prague" ]
    GROUP_NAME = "benchmark_group"
    BOOKING_REQUESTS_QUEUE = "booking_requests"
    BOOKINGS_QUEUE = "bookings"

    # Default request limit
    request_limit = 100

    # Parse command line arguments for request limit
    OptionParser.new do |parser|
      parser.on("-l", "--limit=LIMIT", Integer, "Request limit (default: 100)") do |l|
        request_limit = l
      end
    end.parse!(ARGV)

    puts "Benchmarking with request limit: #{request_limit}"

    original_amqp_url = ENV.fetch("AMQP_URL")
    parsed_uri = URI.parse(original_amqp_url)

    # CloudAMQP often uses the username as the vhost.
    # If the vhost is missing or is '/', we'll try to infer it from the username.
    if parsed_uri.path.nil? || parsed_uri.path == "/" || parsed_uri.path.empty?
      vhost = parsed_uri.user
      parsed_uri.path = "/#{vhost}" if vhost
      puts "Inferred vhost from username: #{vhost}. New AMQP_URL: #{parsed_uri}"
    end

    # Adjust port for AMQPS if it's 5672, as 5671 is the standard TLS port
    if parsed_uri.scheme == "amqps" && parsed_uri.port == 5672
      parsed_uri.port = 5671
      puts "Adjusted AMQP_URL for AMQPS to use port 5671: #{parsed_uri}"
    end
    AMQP_URL = parsed_uri.to_s

    # Helper to publish messages
    def publish_booking_request(connection_or_client, from_city, to_city, queue_name)
      message = { from: from_city, to: to_city, group_name: GROUP_NAME }.to_json
      if connection_or_client.is_a?(Bunny::Session)
        channel = connection_or_client.create_channel
        exchange = channel.exchange("")
        exchange.publish(message, key: queue_name)
        channel.close
      elsif connection_or_client.is_a?(AMQP::Client)
        queue = connection_or_client.queue(queue_name)
        queue.publish(message)
      end
    end

    # Helper to consume messages
    def consume_messages(connection_or_client, queue_name, expected_count)
      messages = []
      if connection_or_client.is_a?(Bunny::Session)
        channel = connection_or_client.create_channel
        queue = channel.queue(queue_name, durable: true)
        consumer = queue.subscribe(block: false, manual_ack: true) do |_delivery_info, _properties, body|
          messages << body
          channel.ack(_delivery_info.delivery_tag)
        end
        # Wait until all messages are consumed or timeout
        Timeout.timeout(30) do
          sleep 0.1 until messages.count >= expected_count
        end
        consumer.cancel
        channel.close
      elsif connection_or_client.is_a?(AMQP::Client)
        queue = connection_or_client.queue(queue_name)
        queue.subscribe do |msg|
          messages << msg.body
          msg.ack
        end
        # Wait until all messages are consumed or timeout
        Timeout.timeout(30) do
          sleep 0.1 until messages.count >= expected_count
        end
        # For AMQP::Client, explicit cancellation of individual consumers is not directly supported
        # in the same way as Bunny. Relying on client.stop for cleanup.
      end
      messages
    end

    # Simulate the microservice processing and publishing
    # For benchmarking, we need to ensure the messages are processed.
    # This part would typically be a separate process.
    # For a self-contained benchmark, we'll simulate it here.
    # This is a simplified version of task1a logic for benchmarking.
    puts "Iterations per second measures:"
    Benchmark.ips do |x|
      x.warmup = 5
      x.time = 10
      x.iterations = 3

      # Bunny Benchmark
      x.report("Bunny Task 1a") do
        bunny_connection = Bunny.new(AMQP_URL, verify_peer: true, tls_silence_warnings: true)
        bunny_connection.start

        # Clear queues before benchmark
        bunny_channel = bunny_connection.create_channel
        bunny_channel.queue(BOOKING_REQUESTS_QUEUE, durable: true).purge
        bunny_channel.queue(BOOKINGS_QUEUE, durable: true).purge
        bunny_channel.close

        request_limit.times do
          from = CITIES.sample
          to = CITIES.sample
          publish_booking_request(bunny_connection, from, to, BOOKING_REQUESTS_QUEUE)
        end

        # Simulate the microservice processing and publishing
        bunny_channel_consumer = bunny_connection.create_channel
        bunny_queue_consumer = bunny_channel_consumer.queue(BOOKING_REQUESTS_QUEUE, durable: true)
        bunny_exchange_publisher = bunny_channel_consumer.exchange("")

        consumed_requests = []
        bunny_consumer = bunny_queue_consumer.subscribe(block: false, manual_ack: true) do |_delivery_info, _properties, body|
          consumed_requests << body
          booking_request = JSON.parse(body)
          from = booking_request.fetch("from")
          to = booking_request.fetch("to")
          booking = { from:, to:, group_name: GROUP_NAME, distance: rand(31337) }
          bunny_exchange_publisher.publish(booking.to_json, key: BOOKINGS_QUEUE)
          bunny_channel_consumer.ack(_delivery_info.delivery_tag)
        end

        Timeout.timeout(30) do
          sleep 0.1 until consumed_requests.count >= request_limit
        end
        bunny_consumer.cancel
        bunny_channel_consumer.close

        consume_messages(bunny_connection, BOOKINGS_QUEUE, request_limit)

        bunny_connection.close
      end

      # AMQP-client Benchmark
      x.report("AMQP-client Task 1a") do
        amqp_client = AMQP::Client.new(AMQP_URL)
        amqp_client.start

        # Clear queues before benchmark
        amqp_client.queue(BOOKING_REQUESTS_QUEUE).purge
        amqp_client.queue(BOOKINGS_QUEUE).purge

        request_limit.times do
          from = CITIES.sample
          to = CITIES.sample
          publish_booking_request(amqp_client, from, to, BOOKING_REQUESTS_QUEUE)
        end

        # Simulate the microservice processing and publishing
        amqp_request_queue_consumer = amqp_client.queue(BOOKING_REQUESTS_QUEUE)
        amqp_bookings_queue_publisher = amqp_client.queue(BOOKINGS_QUEUE)

        consumed_requests = []
        amqp_request_queue_consumer.subscribe do |msg|
          consumed_requests << msg.body
          booking_request = JSON.parse(msg.body)
          from = booking_request.fetch("from")
          to = booking_request.fetch("to")
          booking = { from:, to:, group_name: GROUP_NAME, distance: rand(31337) }
          amqp_bookings_queue_publisher.publish(booking.to_json)
          msg.ack
        end

        Timeout.timeout(30) do
          sleep 0.1 until consumed_requests.count >= request_limit
        end

        consume_messages(amqp_client, BOOKINGS_QUEUE, request_limit)

        amqp_client.stop
      end

      x.compare!
      x.hold! "benchmark_report"
    end
  end
end
