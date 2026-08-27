# Multi-stage Dockerfile for building a single service in this multi-module Maven project
# Usage: docker build --build-arg SERVICE=payment-service -t my-payment-service:latest .
# Default SERVICE is 'payment-service'. Change ARG to build/run other modules (api-gateway, fraud-service, ...)

FROM maven:3.9.4-eclipse-temurin-21 AS build
ARG SERVICE=payment-service
WORKDIR /workspace

# Copy the root pom and modules first to leverage Docker cache
COPY pom.xml ./
COPY settings.xml ./ || true
# Copy everything (module sources) so Maven can build the selected module with its dependencies
COPY . .

# Build only the requested service (and required modules) to speed up build
RUN mvn -B -T1C -DskipTests -pl ${SERVICE} -am package

# Runtime image
FROM eclipse-temurin:21-jre
ARG SERVICE=payment-service
WORKDIR /app

# Copy the built jar from the build stage. This assumes the module produces a Spring Boot runnable jar.
# We try to match typical Spring Boot jar names including version/SNAPSHOT. Adjust the glob if needed.
COPY --from=build /workspace/${SERVICE}/target/*-SNAPSHOT.jar ./app.jar

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "/app/app.jar"]
