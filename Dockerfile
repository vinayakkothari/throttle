# Build
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
COPY throttle-core throttle-core
COPY throttle-server throttle-server
COPY throttle-client throttle-client
COPY throttle-bench throttle-bench
RUN mvn -B -q -DskipTests -pl throttle-server -am package

# Run
FROM eclipse-temurin:21-jre
RUN useradd --system --uid 10001 throttle
COPY --from=build /src/throttle-server/target/throttle-server.jar /app/throttle-server.jar
USER throttle
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/throttle-server.jar"]
