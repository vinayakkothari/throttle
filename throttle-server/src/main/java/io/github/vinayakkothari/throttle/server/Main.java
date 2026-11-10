package io.github.vinayakkothari.throttle.server;

/** Starts the server with settings from the environment. See {@link ServerConfig}. */
public final class Main {
  private Main() {}

  public static void main(String[] args) throws Exception {
    ThrottleServer server = new ThrottleServer(ServerConfig.fromEnv(System.getenv())).start();
    Runtime.getRuntime().addShutdownHook(new Thread(server::close, "shutdown"));
    Thread.currentThread().join();
  }
}
