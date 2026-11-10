package io.github.vinayakkothari.throttle.client;

import io.github.vinayakkothari.throttle.core.Decision;
import io.github.vinayakkothari.throttle.core.Json;
import io.github.vinayakkothari.throttle.core.Rule;
import io.github.vinayakkothari.throttle.core.UnknownRuleException;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Talks to one or more throttle servers. Requests go round robin, and a server that fails is
 * skipped in favour of the next one before the call is given up on.
 */
public final class HttpBackend implements Backend {
  private final List<URI> servers;
  private final HttpClient http;
  private final Duration timeout;
  private final AtomicInteger next = new AtomicInteger();

  public HttpBackend(List<String> servers, Duration timeout) {
    if (servers.isEmpty()) throw new IllegalArgumentException("at least one server is needed");
    this.servers = servers.stream().map(s -> URI.create(s.replaceAll("/+$", ""))).toList();
    this.timeout = timeout;
    this.http =
        HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(timeout)
            .executor(Executors.newVirtualThreadPerTaskExecutor())
            .build();
  }

  public static HttpBackend of(String... servers) {
    return new HttpBackend(List.of(servers), Duration.ofMillis(250));
  }

  @Override
  public Decision acquire(String rule, String key, long want, boolean partial) {
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("rule", rule);
    body.put("key", key);
    body.put(partial ? "want" : "cost", want);
    HttpResponse<String> res = send(partial ? "/v1/acquire" : "/v1/check", Json.write(body));
    if (res.statusCode() == 404) throw new UnknownRuleException(rule);
    if (res.statusCode() == 400) {
      throw new IllegalArgumentException(String.valueOf(Json.parseObject(res.body()).get("error")));
    }
    return Decision.fromJson(Json.parseObject(res.body()));
  }

  @Override
  public List<Rule> rules() {
    HttpResponse<String> res = send("/v1/rules", null);
    List<Rule> out = new ArrayList<>();
    Object list = Json.parseObject(res.body()).get("rules");
    if (list instanceof List<?> items) {
      for (Object o : items) {
        @SuppressWarnings("unchecked")
        Map<String, Object> m = (Map<String, Object>) o;
        out.add(Rule.fromJson(m, null));
      }
    }
    return out;
  }

  /** Try each server once, starting from the next in the rotation. */
  private HttpResponse<String> send(String path, String body) {
    int start = Math.floorMod(next.getAndIncrement(), servers.size());
    Exception last = null;
    for (int i = 0; i < servers.size(); i++) {
      URI base = servers.get((start + i) % servers.size());
      HttpRequest.Builder req =
          HttpRequest.newBuilder(base.resolve(path)).timeout(timeout).header("Accept", "application/json");
      if (body == null) req.GET();
      else req.header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(body));
      try {
        HttpResponse<String> res = http.send(req.build(), HttpResponse.BodyHandlers.ofString());
        if (res.statusCode() < 500) return res;
        last = new IOException(base + " answered " + res.statusCode());
      } catch (IOException e) {
        last = e;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new BackendException("interrupted", e);
      }
    }
    throw new BackendException("no throttle server answered: " + last.getMessage(), last);
  }

  @Override
  public void close() {
    http.close();
  }
}
