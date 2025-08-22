package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.sevice.BatchProcessingService;

public class WeatherBatchProcessor {

    // Đọc config từ environment variables
    private static final String KAFKA_BOOTSTRAP_SERVERS = getEnvOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    private static final String KAFKA_TOPIC = getEnvOrDefault("KAFKA_TOPIC", "weather-topic");
    private static final String MINIO_ENDPOINT = getEnvOrDefault("MINIO_ENDPOINT", "http://localhost:9000");
    private static final String MINIO_ACCESS_KEY = getEnvOrDefault("MINIO_ACCESS_KEY", "minioadmin");
    private static final String MINIO_SECRET_KEY = getEnvOrDefault("MINIO_SECRET_KEY", "minioadmin123");
    private static final String MINIO_BUCKET = getEnvOrDefault("MINIO_BUCKET", "weather-bucket");
    private static final String MINIO_BASE_PATH = "s3a://" + MINIO_BUCKET + "/batch-views";

    public static void main(String[] args) {
        SparkSession spark = null;

        try {
            print_info("=== Starting Weather Batch Processing ===");
            print_info("Kafka Bootstrap Servers: " + KAFKA_BOOTSTRAP_SERVERS);
            print_info("Kafka Topic: " + KAFKA_TOPIC);
            print_info("MinIO Endpoint: " + MINIO_ENDPOINT);
            print_info("MinIO Bucket: " + MINIO_BUCKET);

            // Khởi tạo Spark Session với MinIO config
            spark = createSparkSessionWithMinIO();
            spark.sparkContext().setLogLevel("WARN");

            print_info("Spark Session created successfully");

            // Khởi tạo service
            BatchProcessingService batchService = new BatchProcessingService(spark);

            // Đọc dữ liệu từ Kafka
            print_info("Reading data from Kafka topic: " + KAFKA_TOPIC);
            Dataset<Row> rawWeatherData = batchService.readFromKafka(KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC);

            // Cache data để tối ưu performance
            rawWeatherData.cache();

            long totalRecords = rawWeatherData.count();
            print_info("Total records processed: " + totalRecords);

            if (totalRecords > 0) {
                // Hiển thị sample data
                print_info("Sample raw data:");
                rawWeatherData.show(10, false);

                // Xử lý hourly aggregations
                print_info("Processing hourly aggregations...");
                Dataset<Row> hourlyAggregations = batchService.processHourlyAggregations(rawWeatherData);
                print_info("Hourly aggregations sample:");
                hourlyAggregations.show(10, false);

                // Xử lý daily aggregations
                print_info("Processing daily aggregations...");
                Dataset<Row> dailyAggregations = batchService.processDailyAggregations(rawWeatherData);
                print_info("Daily aggregations sample:");
                dailyAggregations.show(10, false);

                // Lưu vào MinIO
                print_info("Saving aggregated data to MinIO...");
                batchService.saveAggregatedData(hourlyAggregations, dailyAggregations, MINIO_BASE_PATH);

                print_info("=== Batch Processing Completed Successfully ===");
                print_info("Data saved to MinIO bucket: " + MINIO_BUCKET);

            } else {
                print_warning("No data found in Kafka topic: " + KAFKA_TOPIC);
                print_warning("Make sure the producer is running and sending data");
            }

        } catch (Exception e) {
            print_error("Error during batch processing: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        } finally {
            if (spark != null) {
                print_info("Stopping Spark Session...");
                spark.stop();
            }
        }
    }

    private static SparkSession createSparkSessionWithMinIO() {
        return SparkSession.builder()
                .appName("Weather-Batch-Processor")
                .master(getEnvOrDefault("SPARK_MASTER", "local[*]"))
                // MinIO/S3 Configuration
                .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
                .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
                .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                // Memory optimization
                .config("spark.sql.execution.arrow.pyspark.enabled", "true")
                .config("spark.sql.adaptive.skewJoin.enabled", "true")
                .getOrCreate();
    }

    // Utility methods
    private static String getEnvOrDefault(String envKey, String defaultValue) {
        String value = System.getenv(envKey);
        return (value != null && !value.isEmpty()) ? value : defaultValue;
    }

    private static void print_info(String message) {
        System.out.println("[INFO] " + java.time.LocalDateTime.now() + " - " + message);
    }

    private static void print_warning(String message) {
        System.out.println("[WARNING] " + java.time.LocalDateTime.now() + " - " + message);
    }

    private static void print_error(String message) {
        System.err.println("[ERROR] " + java.time.LocalDateTime.now() + " - " + message);
    }
}