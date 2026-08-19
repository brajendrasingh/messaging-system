# =========================
# Build Stage
# =========================
FROM maven:3.9-eclipse-temurin-25 AS builder

WORKDIR /app

# Copy pom first for better Docker layer caching
COPY pom.xml .

# Download dependencies
RUN mvn dependency:go-offline -B

# Copy source code
COPY src ./src

# Build Spring Boot application
RUN mvn clean package -DskipTests


# =========================
# Runtime Stage
# =========================
FROM eclipse-temurin:25-jre

WORKDIR /app

# Copy generated JAR
COPY --from=builder /app/target/*.jar app.jar

# Spring Boot port
EXPOSE 8083

# Start application
ENTRYPOINT ["java", "-jar", "app.jar"]