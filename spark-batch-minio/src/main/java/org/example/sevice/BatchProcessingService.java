package org.example.sevice;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static org.apache.spark.sql.functions.*;

public class BatchProcessingService {
    private final SparkSession spark;

    public BatchProcessingService(SparkSession spark) {
        this.spark = spark;
    }

    // Schema cho weather data theo CSV header
    public StructType getWeatherSchema() {
        return new StructType(new StructField[]{
                DataTypes.createStructField("Formatted Date", DataTypes.TimestampType, false),
                DataTypes.createStructField("Summary", DataTypes.StringType, true),
                DataTypes.createStructField("Precip Type", DataTypes.StringType, true),
                DataTypes.createStructField("Temperature (C)", DataTypes.DoubleType, true),
                DataTypes.createStructField("Apparent Temperature (C)", DataTypes.DoubleType, true),
                DataTypes.createStructField("Humidity", DataTypes.DoubleType, true),
                DataTypes.createStructField("Wind Speed (km/h)", DataTypes.DoubleType, true),
                DataTypes.createStructField("Wind Bearing (degrees)", DataTypes.DoubleType, true),
                DataTypes.createStructField("Visibility (km)", DataTypes.DoubleType, true),
                DataTypes.createStructField("Loud Cover", DataTypes.DoubleType, true),
                DataTypes.createStructField("Pressure (millibars)", DataTypes.DoubleType, true),
                DataTypes.createStructField("Daily Summary", DataTypes.StringType, true)
        });
    }

    // Đọc từ CSV với header chính xác
    public Dataset<Row> readFromCSV(String csvPath) {
        return spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .option("timestampFormat", "yyyy-MM-dd HH:mm:ss.SSS Z")
                .csv(csvPath)
                // Clean column names - loại bỏ space và ký tự đặc biệt cho dễ xử lý
                .withColumnRenamed("Formatted Date", "formatted_date")
                .withColumnRenamed("Summary", "summary")
                .withColumnRenamed("Precip Type", "precip_type")
                .withColumnRenamed("Temperature (C)", "temperature")
                .withColumnRenamed("Apparent Temperature (C)", "apparent_temperature")
                .withColumnRenamed("Humidity", "humidity")
                .withColumnRenamed("Wind Speed (km/h)", "wind_speed")
                .withColumnRenamed("Wind Bearing (degrees)", "wind_bearing")
                .withColumnRenamed("Visibility (km)", "visibility")
                .withColumnRenamed("Loud Cover", "loud_cover")
                .withColumnRenamed("Pressure (millibars)", "pressure")
                .withColumnRenamed("Daily Summary", "daily_summary");
    }

    // Đọc dữ liệu từ Kafka (với schema đã clean)
    public Dataset<Row> readFromKafka(String bootstrapServers, String topic) {
        return spark.read()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("subscribe", topic)
                .option("startingOffsets", "earliest")
                .option("endingOffsets", "latest")
                .load()
                .select(
                        from_json(col("value").cast("string"), getWeatherSchema()).as("data"),
                        col("timestamp").as("kafka_timestamp")
                )
                .select("data.*", "kafka_timestamp")
                // Clean column names cho consistent
                .withColumnRenamed("Formatted Date", "formatted_date")
                .withColumnRenamed("Summary", "summary")
                .withColumnRenamed("Precip Type", "precip_type")
                .withColumnRenamed("Temperature (C)", "temperature")
                .withColumnRenamed("Apparent Temperature (C)", "apparent_temperature")
                .withColumnRenamed("Humidity", "humidity")
                .withColumnRenamed("Wind Speed (km/h)", "wind_speed")
                .withColumnRenamed("Wind Bearing (degrees)", "wind_bearing")
                .withColumnRenamed("Visibility (km)", "visibility")
                .withColumnRenamed("Loud Cover", "loud_cover")
                .withColumnRenamed("Pressure (millibars)", "pressure")
                .withColumnRenamed("Daily Summary", "daily_summary");
    }

    // Xử lý aggregations theo giờ
    public Dataset<Row> processHourlyAggregations(Dataset<Row> weatherData) {
        return weatherData
                .withColumn("hour", date_trunc("hour", col("formatted_date")))
                .groupBy(col("hour"))
                .agg(
                        avg("temperature").alias("avg_temperature"),
                        min("temperature").alias("min_temperature"),
                        max("temperature").alias("max_temperature"),
                        avg("apparent_temperature").alias("avg_apparent_temp"),
                        avg("humidity").alias("avg_humidity"),
                        avg("wind_speed").alias("avg_wind_speed"),
                        max("wind_speed").alias("max_wind_speed"),
                        avg("wind_bearing").alias("avg_wind_bearing"),
                        avg("visibility").alias("avg_visibility"),
                        avg("loud_cover").alias("avg_loud_cover"),
                        avg("pressure").alias("avg_pressure"),
                        min("pressure").alias("min_pressure"),
                        max("pressure").alias("max_pressure"),
                        count("*").alias("record_count"),
                        collect_set("summary").alias("weather_summaries"),
                        collect_set("precip_type").alias("precip_types")
                )
                .orderBy("hour");
    }

    // Xử lý aggregations theo ngày
    public Dataset<Row> processDailyAggregations(Dataset<Row> weatherData) {
        return weatherData
                .withColumn("date", to_date(col("formatted_date")))
                .groupBy(col("date"))
                .agg(
                        avg("temperature").alias("avg_temperature"),
                        min("temperature").alias("min_temperature"),
                        max("temperature").alias("max_temperature"),
                        avg("apparent_temperature").alias("avg_apparent_temp"),
                        min("apparent_temperature").alias("min_apparent_temp"),
                        max("apparent_temperature").alias("max_apparent_temp"),
                        avg("humidity").alias("avg_humidity"),
                        min("humidity").alias("min_humidity"),
                        max("humidity").alias("max_humidity"),
                        avg("wind_speed").alias("avg_wind_speed"),
                        max("wind_speed").alias("max_wind_speed"),
                        avg("wind_bearing").alias("avg_wind_bearing"),
                        avg("visibility").alias("avg_visibility"),
                        min("visibility").alias("min_visibility"),
                        avg("loud_cover").alias("avg_loud_cover"),
                        avg("pressure").alias("avg_pressure"),
                        min("pressure").alias("min_pressure"),
                        max("pressure").alias("max_pressure"),
                        count("*").alias("record_count"),
                        collect_set("summary").alias("daily_weather_summaries")
                )
                .orderBy("date");
    }

    // Thêm method để xử lý monthly aggregations
    public Dataset<Row> processMonthlyAggregations(Dataset<Row> weatherData) {
        return weatherData
                .withColumn("year_month", date_trunc("month", col("formatted_date")))
                .groupBy(col("year_month"))
                .agg(
                        avg("temperature").alias("monthly_avg_temp"),
                        min("temperature").alias("monthly_min_temp"),
                        max("temperature").alias("monthly_max_temp"),
                        avg("humidity").alias("monthly_avg_humidity"),
                        avg("wind_speed").alias("monthly_avg_wind_speed"),
                        avg("pressure").alias("monthly_avg_pressure"),
                        count("*").alias("monthly_record_count")
                )
                .orderBy("year_month");
    }

    // Lưu vào MinIO
    public void saveToMinIO(Dataset<Row> data, String bucketPath, String partitionColumn) {
        data.coalesce(1)
                .write()
                .mode("append")
                .option("compression", "snappy")
                .partitionBy(partitionColumn)
                .parquet(bucketPath);

        System.out.println("Data saved to MinIO: " + bucketPath);
    }

    // Lưu aggregated data
    public void saveAggregatedData(Dataset<Row> hourlyData, Dataset<Row> dailyData, String basePath) {
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmm"));

        // Lưu hourly aggregations
        String hourlyPath = basePath + "/hourly/" + timestamp;
        saveToMinIO(hourlyData, hourlyPath, "hour");

        // Lưu daily aggregations
        String dailyPath = basePath + "/daily/" + timestamp;
        saveToMinIO(dailyData, dailyPath, "date");

        System.out.println("Batch processing completed at: " + timestamp);
    }
}
