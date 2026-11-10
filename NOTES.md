# Build notes

A running log while building throttle. What broke, what surprised me, what the numbers said.

## Week 2

- The JDK's HTTP server leaves Nagle's algorithm on. Small JSON replies on keep-alive connections waited on delayed ACKs, at 44 ms per call on localhost. Setting `sun.net.httpserver.nodelay=true` brought that to 1.6 ms. The two-server test went from 2.6 s to 0.4 s.
- The sliding window counter clamped retry-after to at least 1 ms after setting it to -1 for "never possible", so an impossible request was told to retry in 1 ms. A smoke test caught it before any unit test existed.
- I expected leasing to overshoot the limit by up to `rate × TTL`. It didn't in the benchmark, because busy servers spend leased tokens immediately. No second went past 1,100. The bound is real, but it takes leases that sit unspent while the bucket refills.
- I expected quiet servers to waste most of their leases. Partial grants mean a lease only gets what is left when the bucket is under pressure, so waste was at most 95 tokens out of 5,100. It varies between runs, and one run wasted none.
- The negative cache turned out to matter as much as leasing for exhausted keys. About 32 backend calls per 1,000 decisions with leasing off.
- My first outage test made calls while Redis was cut, which happened to empty the connection pool. Cutting and restoring between requests left the pool full of dead connections, and the next decision came back degraded even though Redis was fine. I wrote that test first, watched it fail, then made the client drop all idle connections and retry once on a fresh one.
-
