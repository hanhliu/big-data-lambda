# --- Build stage ---
FROM maven:3.9.8-eclipse-temurin-21 AS builder
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn clean package -DskipTests

# --- Runtime stage ---
FROM eclipse-temurin:21-jdk
WORKDIR /opt/spark-app
COPY --from=builder /app/target/spark-streaming-consumer-1.0-SNAPSHOT.jar spark-streaming-consumer.jar

# Thêm connector cho Kafka + Hadoop AWS nếu cần
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar /opt/spark-app/
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark-app/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /opt/spark-app/

ENTRYPOINT ["java","-jar","/opt/spark-app/spark-streaming-consumer.jar"]
