package io.github.vinayakkothari.throttle.bench;

import io.github.vinayakkothari.throttle.client.Backend;
import io.github.vinayakkothari.throttle.client.ClientStats;
import io.github.vinayakkothari.throttle.client.HttpBackend;
import io.github.vinayakkothari.throttle.client.RedisBackend;
import io.github.vinayakkothari.throttle.client.ThrottleClient;
import io.github.vinayakkothari.throttle.core.Rule;
import io.github.vinayakkothari.throttle.core.redis.RedisClient;
import io.github.vinayakkothari.throttle.server.ServerConfig;
import io.github.vinayakkothari.throttle.server.ThrottleServer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Measures what leasing buys and what it costs.
 *
 * <p>Part 1, speed: decisions per second and latency for library mode, the service with one call
 * per decision, and the service with leases.
 *
 * <p>Part 2, accuracy: four simulated app servers share a limit of 1,000 a second. With all four
 * busy, does leasing ever let through more than the limit? With one busy and three quiet, how
 * much of the limit do the quiet ones waste by leasing tokens they never spend?
 *
 * <pre>
 *   java -cp ... io.github.vinayakkothari.throttle.bench.Bench [--seconds 10] [--workers 64]
 *        [--redis redis://localhost:6379] [--out bench/RESULTS.md]
 * </pre>
 */
public final class Bench {
  private final String redisUrl;
  private final int seconds;
  private final int workers;
  private final String prefix = "bench-" + UUID.randomUUID().toString().substring(0, 6);
  private ThrottleServer a;
  private ThrottleServer b;
  private RedisClient stats;

  private Bench(String redisUrl, int seconds, int workers) {
    this.redisUrl = redisUrl;
    this.seconds = seconds;
    this.workers = workers;
  }

  public static void main(String[] args) throws Exception {
    String redis = "redis://localhost:6379";
    int seconds = 10;
    int workers = 64;
    Path out = Path.of("bench", "RESULTS.md");
    for (int i = 0; i + 1 < args.length; i += 2) {
      switch (args[i]) {
        case "--redis" -> redis = args[i + 1];
        case "--seconds" -> seconds = Integer.parseInt(args[i + 1]);
        case "--workers" -> workers = Integer.parseInt(args[i + 1]);
        case "--out" -> out = Path.of(args[i + 1]);
        default -> throw new IllegalArgumentException("unknown option " + args[i]);
      }
    }
    Bench bench = new Bench(redis, seconds, workers);
    String report = bench.run();
    if (out.getParent() != null) Files.createDirectories(out.getParent());
    Files.writeString(out, report);
    System.out.println("\nwrote " + out);
  }

  private String run() throws Exception {
    ServerConfig cfg =
        ServerConfig.defaults().withPort(0).withRedisUrl(redisUrl).withPrefix(prefix);
    a = new ThrottleServer(cfg).start();
    b = new ThrottleServer(cfg).start();
    stats = RedisClient.create(redisUrl);
    a.rules().put(Rule.tokenBucket("open", 1_000_000_000L, Duration.ofSeconds(1), 1_000_000_000L));
    a.rules().put(Rule.tokenBucket("shared", 1_000, Duration.ofSeconds(1), 100));
    long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
    while (b.rules().get("shared").isEmpty() && System.nanoTime() < deadline) {
      Thread.sleep(10); // server b hears about the rules through pub/sub
    }
    try {
      StringBuilder md = new StringBuilder();
      md.append("# Benchmark results\n\n");
      md.append(String.format(Locale.ROOT,
          "Run on %s. %d CPU cores, Java %s, Redis %s on the same machine. Both throttle servers "
              + "run in the benchmark's own JVM, so client, servers and Redis all share the same "
              + "cores. Absolute numbers will be higher on real hardware; the ratios are the point.%n%n",
          LocalDate.now(), Runtime.getRuntime().availableProcessors(),
          System.getProperty("java.version"), redisVersion()));
      speed(md);
      accuracy(md);
      return md.toString();
    } finally {
      a.close();
      b.close();
      stats.close();
    }
  }

  // --------------------------------------------------------------------------- speed

  private record SpeedResult(
      String mode, long decisions, double seconds, Latency latency, ClientStats client,
      long redisCalls) {}

  private void speed(StringBuilder md) throws Exception {
    System.out.printf("speed: %d workers, %d s per mode, 1,000 tenants%n", workers, seconds);
    List<SpeedResult> results = new ArrayList<>();
    results.add(speed("Library mode, straight to Redis",
        () -> ThrottleClient.builder(new RedisBackend(RedisClient.create(redisUrl), prefix)).build()));
    results.add(speed("Service, one call per decision", () -> client(1)));
    results.add(speed("Service, lease 10", () -> client(10)));
    results.add(speed("Service, lease 100", () -> client(100)));

    md.append("## Speed\n\n");
    md.append(String.format(Locale.ROOT,
        "%d workers on virtual threads call `tryAcquire` in a loop for %d seconds per mode, "
            + "spread over 1,000 tenant keys, against a limit high enough that every call is "
            + "allowed. Latency is measured around each `tryAcquire` call.%n%n",
        workers, seconds));
    md.append("| Mode | Decisions/s | p50 | p99 | p99.9 | Backend calls per 1,000 | "
        + "Redis scripts per 1,000 |\n");
    md.append("| --- | --- | --- | --- | --- | --- | --- |\n");
    for (SpeedResult r : results) {
      md.append(String.format(Locale.ROOT, "| %s | %,.0f | %s | %s | %s | %.0f | %.0f |%n",
          r.mode, r.decisions / r.seconds, ms(r.latency.percentile(50)),
          ms(r.latency.percentile(99)), ms(r.latency.percentile(99.9)),
          r.client.callsPerThousand(), 1000.0 * r.redisCalls / Math.max(1, r.decisions)));
    }
    md.append('\n');
  }

  private ThrottleClient client(int lease) {
    Backend backend =
        new HttpBackend(List.of(url(a), url(b)), Duration.ofSeconds(2));
    return ThrottleClient.builder(backend)
        .leaseSize(lease)
        .leaseTtl(Duration.ofMillis(100))
        .build();
  }

  private SpeedResult speed(String mode, Supplier<ThrottleClient> factory) throws Exception {
    long redisBefore = evalCalls();
    AtomicLong decisions = new AtomicLong();
    List<Latency> perWorker = new ArrayList<>();
    long start;
    ClientStats clientStats;
    try (ThrottleClient c = factory.get()) {
      // warm up: JIT, connection pools, script cache
      runFor(Duration.ofSeconds(2), 16, i -> c.tryAcquire("open", "warm-" + (i % 1000)), null, null);
      redisBefore = evalCalls();
      long baseCalls = c.stats().backendCalls();
      long baseDecisions = c.stats().decisions();
      start = System.nanoTime();
      runFor(Duration.ofSeconds(seconds), workers,
          i -> c.tryAcquire("open", "tenant-" + ThreadLocalRandom.current().nextInt(1000)),
          decisions, perWorker);
      ClientStats s = c.stats();
      clientStats =
          new ClientStats(s.decisions() - baseDecisions, s.backendCalls() - baseCalls,
              s.servedFromLease(), s.expiredTokens(), s.negativeCacheHits(), s.fallbacks(),
              s.breaker());
    }
    double elapsed = (System.nanoTime() - start) / 1e9;
    Latency all = new Latency();
    perWorker.forEach(all::merge);
    long redisCalls = evalCalls() - redisBefore;
    System.out.printf(Locale.ROOT, "  %-34s %,10.0f/s  p50 %.3f ms  p99 %.3f ms%n",
        mode, decisions.get() / elapsed, all.percentile(50), all.percentile(99));
    return new SpeedResult(mode, decisions.get(), elapsed, all, clientStats, redisCalls);
  }

  // ------------------------------------------------------------------------ accuracy

  private record AccuracyResult(
      String traffic, int lease, long allowed, long ideal, long busiestSecond,
      double callsPerThousand, long expired) {}

  private void accuracy(StringBuilder md) throws Exception {
    int secs = Math.max(3, seconds / 2);
    long ideal = 100 + 1_000L * secs; // burst plus rate times time
    System.out.printf("accuracy: limit 1,000/s burst 100, 4 app servers, %d s per run%n", secs);
    List<AccuracyResult> results = new ArrayList<>();
    for (String traffic : List.of("all busy", "one busy, three quiet")) {
      for (int lease : new int[] {1, 10, 100}) {
        results.add(accuracy(traffic, lease, secs, ideal));
      }
    }

    md.append("## Accuracy\n\n");
    md.append(String.format(Locale.ROOT,
        "Four `ThrottleClient` instances stand in for four app servers sharing one key with a "
            + "limit of 1,000 a second and a burst of 100, for %d seconds. The most the limit "
            + "can allow in that time is 100 + 1,000 x %d = %,d. Leases expire after 100 ms.%n%n"
            + "**All busy**: every server sends as fast as it can. This is where leasing could "
            + "overshoot.%n%n"
            + "**One busy, three quiet**: one server sends as fast as it can, three send 50 "
            + "requests a second each. Quiet servers lease more than they spend, and their "
            + "unspent tokens are lost to the busy one.%n%n"
            + "**Busiest second** is the most requests allowed in any 1,000 ms stretch of the "
            + "run. An exact limiter allows at most the burst plus one second of refill, 1,100. "
            + "Leasing can push that to 1,100 + 1,000 x 0.1 s = 1,200 at worst, because a "
            + "token taken now may be spent up to 100 ms later.%n%n",
        secs, secs, ideal));
    md.append("| Traffic | Lease | Allowed | Of the most possible | Busiest second "
        + "| Backend calls per 1,000 decisions | Leased tokens expired unused |\n");
    md.append("| --- | --- | --- | --- | --- | --- | --- |\n");
    for (AccuracyResult r : results) {
      md.append(String.format(Locale.ROOT, "| %s | %s | %,d | %.1f%% | %,d | %.1f | %,d |%n",
          r.traffic, r.lease == 1 ? "none" : r.lease, r.allowed, 100.0 * r.allowed / r.ideal,
          r.busiestSecond, r.callsPerThousand, r.expired));
    }
    md.append('\n');
  }

  private AccuracyResult accuracy(String traffic, int lease, int secs, long ideal)
      throws Exception {
    String key = "acc-" + UUID.randomUUID();
    boolean skewed = traffic.startsWith("one");
    List<ThrottleClient> clients = new ArrayList<>();
    for (int i = 0; i < 4; i++) clients.add(client(lease));
    AtomicLong allowed = new AtomicLong();
    Queue<Long> times = new ConcurrentLinkedQueue<>();
    long deadline = System.nanoTime() + Duration.ofSeconds(secs).toNanos();
    try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
      List<Future<?>> tasks = new ArrayList<>();
      for (int i = 0; i < 4; i++) {
        ThrottleClient c = clients.get(i);
        boolean quiet = skewed && i > 0;
        int threads = quiet ? 1 : 8;
        for (int t = 0; t < threads; t++) {
          tasks.add(pool.submit(() -> {
            while (System.nanoTime() < deadline) {
              if (c.tryAcquire("shared", key).allowed()) {
                allowed.incrementAndGet();
                times.add(System.nanoTime());
              }
              if (quiet) Thread.sleep(20);
            }
            return null;
          }));
        }
      }
      for (Future<?> f : tasks) f.get();
    }
    long decisions = 0;
    long calls = 0;
    long expired = 0;
    for (ThrottleClient c : clients) {
      ClientStats s = c.stats();
      decisions += s.decisions();
      calls += s.backendCalls();
      expired += s.expiredTokens();
      c.close();
    }
    double perThousand = decisions == 0 ? 0 : 1000.0 * calls / decisions;
    long busiest = busiestSecond(times);
    System.out.printf(Locale.ROOT,
        "  %-22s lease %-4d allowed %,7d of %,d (%.1f%%)  busiest second %,d%n",
        traffic, lease, allowed.get(), ideal, 100.0 * allowed.get() / ideal, busiest);
    return new AccuracyResult(traffic, lease, allowed.get(), ideal, busiest, perThousand, expired);
  }

  /** Most events inside any one-second stretch, with two pointers over sorted times. */
  private static long busiestSecond(Collection<Long> times) {
    long[] t = times.stream().mapToLong(Long::longValue).sorted().toArray();
    long best = 0;
    int lo = 0;
    for (int hi = 0; hi < t.length; hi++) {
      while (t[hi] - t[lo] >= 1_000_000_000L) lo++;
      best = Math.max(best, hi - lo + 1);
    }
    return best;
  }

  // ------------------------------------------------------------------------- helpers

  private interface Call {
    void run(long i) throws Exception;
  }

  private static void runFor(Duration d, int threads, Call call, AtomicLong count,
      List<Latency> latencies) throws Exception {
    long deadline = System.nanoTime() + d.toNanos();
    try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
      List<Future<Latency>> tasks = new ArrayList<>();
      for (int t = 0; t < threads; t++) {
        tasks.add(pool.submit(() -> {
          Latency lat = new Latency();
          long i = 0;
          while (true) {
            long s = System.nanoTime();
            if (s >= deadline) break;
            call.run(i++);
            lat.record(System.nanoTime() - s);
          }
          if (count != null) count.addAndGet(lat.count());
          return lat;
        }));
      }
      for (Future<Latency> f : tasks) {
        Latency l = f.get();
        if (latencies != null) latencies.add(l);
      }
    }
  }

  private static String ms(double v) {
    return v < 0.001 ? "under 1 µs" : String.format(Locale.ROOT, "%.3f ms", v);
  }

  private static String url(ThrottleServer s) {
    return "http://127.0.0.1:" + s.port();
  }

  private long evalCalls() {
    String info = (String) stats.call("INFO", "commandstats");
    long total = 0;
    Matcher m = Pattern.compile("cmdstat_(evalsha|eval):calls=(\\d+)").matcher(info);
    while (m.find()) total += Long.parseLong(m.group(2));
    return total;
  }

  private String redisVersion() {
    Matcher m = Pattern.compile("redis_version:(\\S+)").matcher((String) stats.call("INFO", "server"));
    return m.find() ? m.group(1) : "unknown";
  }
}
