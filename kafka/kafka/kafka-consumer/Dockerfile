# =========================
# Build Stage
# =========================
FROM gradle:9.4-jdk25 AS builder

WORKDIR /app

# Copy Gradle configuration first
COPY build.gradle.kts .
COPY settings.gradle.kts .

# Download dependencies
RUN gradle dependencies --no-daemon

# Copy source code
COPY src ./src

# Build Spring Boot application
RUN gradle clean bootJar --no-daemon


# =========================
# Runtime Stage
# =========================
FROM eclipse-temurin:25-jre

WORKDIR /app

# Copy Spring Boot JAR
COPY --from=builder /app/build/libs/*.jar app.jar

EXPOSE 8082

ENTRYPOINT ["java", "-jar", "app.jar"]