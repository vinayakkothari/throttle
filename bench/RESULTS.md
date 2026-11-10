# Benchmark results

Run on 2026-11-11. 2 CPU cores, Java 21.0.12.1, Redis 7.0.15 on the same machine. Both throttle servers run in the benchmark's own JVM, so client, servers and Redis all share the same cores. Absolute numbers will be higher on real hardware; the ratios are the point.

## Speed

64 workers on virtual threads call `tryAcquire` in a loop for 10 seconds per mode, spread over 1,000 tenant keys, against a limit high enough that every call is allowed. Latency is measured around each `tryAcquire` call.

| Mode | Decisions/s | p50 | p99 | p99.9 | Backend calls per 1,000 | Redis scripts per 1,000 |
| --- | --- | --- | --- | --- | --- | --- |
| Library mode, straight to Redis | 53,232 | 1.024 ms | 3.323 ms | 6.457 ms | 1000 | 1000 |
| Service, one call per decision | 5,341 | 10.300 ms | 34.500 ms | 59.300 ms | 1000 | 1000 |
| Service, lease 10 | 101,138 | under 1 µs | 7.116 ms | 11.200 ms | 109 | 109 |
| Service, lease 100 | 549,647 | under 1 µs | 3.755 ms | 6.853 ms | 17 | 17 |

## Accuracy

Four `ThrottleClient` instances stand in for four app servers sharing one key with a limit of 1,000 a second and a burst of 100, for 5 seconds. The most the limit can allow in that time is 100 + 1,000 x 5 = 5,100. Leases expire after 100 ms.

**All busy**: every server sends as fast as it can. This is where leasing could overshoot.

**One busy, three quiet**: one server sends as fast as it can, three send 50 requests a second each. Quiet servers lease more than they spend, and their unspent tokens are lost to the busy one.

**Busiest second** is the most requests allowed in any 1,000 ms stretch of the run. An exact limiter allows at most the burst plus one second of refill, 1,100. Leasing can push that to 1,100 + 1,000 x 0.1 s = 1,200 at worst, because a token taken now may be spent up to 100 ms later.

| Traffic | Lease | Allowed | Of the most possible | Busiest second | Backend calls per 1,000 decisions | Leased tokens expired unused |
| --- | --- | --- | --- | --- | --- | --- |
| all busy | none | 5,098 | 100.0% | 1,100 | 31.6 | 0 |
| all busy | 10 | 5,099 | 100.0% | 1,098 | 0.2 | 0 |
| all busy | 100 | 5,100 | 100.0% | 1,099 | 0.2 | 0 |
| one busy, three quiet | none | 5,099 | 100.0% | 1,100 | 2.8 | 0 |
| one busy, three quiet | 10 | 5,078 | 99.6% | 1,082 | 0.3 | 16 |
| one busy, three quiet | 100 | 5,002 | 98.1% | 1,006 | 0.3 | 95 |

