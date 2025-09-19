# LavinMQ workshop exercises

Solved using [bunny](https://github.com/ruby-amqp/bunny) and [amqp-client](https://github.com/cloudamqp/amqp-client.rb).

```shell
export AMQP_URL=amqps://user:pass@hostname:5672

cd bunny
ruby task1a.rb

# or

cd amqp-client
ruby task1a.rb
```

## Benchmarks

### Iterations per second measures

```bash
ruby 3.4.3 (2025-04-14 revision d0b7e5b6a0) +PRISM [arm64-darwin24]
Warming up --------------------------------------
       Bunny Task 1a     1.000 i/100ms
       Bunny Task 1a     1.000 i/100ms
       Bunny Task 1a     1.000 i/100ms
Calculating -------------------------------------
       Bunny Task 1a      0.067 (± 0.0%) i/s    (14.98 s/i) -      1.000 in  14.982748s
       Bunny Task 1a      0.069 (± 0.0%) i/s    (14.55 s/i) -      1.000 in  14.554663s
       Bunny Task 1a      0.074 (± 0.0%) i/s    (13.51 s/i) -      1.000 in  13.513310s

Warming up --------------------------------------
 AMQP-client Task 1a     1.000 i/100ms
 AMQP-client Task 1a     1.000 i/100ms
 AMQP-client Task 1a     1.000 i/100ms
Calculating -------------------------------------
 AMQP-client Task 1a      0.024 (± 0.0%) i/s    (42.00 s/i) -      1.000 in  42.001679s
 AMQP-client Task 1a      0.022 (± 0.0%) i/s    (44.70 s/i) -      1.000 in  44.695359s
 AMQP-client Task 1a      0.023 (± 0.0%) i/s    (43.86 s/i) -      1.000 in  43.861961s

Comparison:
       Bunny Task 1a:        0.1 i/s
 AMQP-client Task 1a:        0.0 i/s - 3.25x  slower
```
