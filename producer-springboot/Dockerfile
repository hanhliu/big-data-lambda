# --- Build stage ---
FROM maven:3.9.8-eclipse-temurin-21 AS builder
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn clean package -DskipTests

# --- Runtime stage ---
FROM eclipse-temurin:21-jdk
WORKDIR /app
COPY --from=builder /app/target/producer-springboot-0.0.1-SNAPSHOT.jar app.jar

EXPOSE 8080
ENTRYPOINT ["java","-jar","/app/app.jar"]
