package org.example.config;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class SparkConfig {

    public static SparkSession createSparkSession(String appName) {
        SparkConf conf = new SparkConf()
                .setAppName(appName)
                .setMaster("local[*]") // Thay đổi theo môi trường của bạn
                // MinIO/S3 Configuration
                .set("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") // MinIO endpoint
                .set("spark.hadoop.fs.s3a.access.key", "minioadmin") // MinIO access key
                .set("spark.hadoop.fs.s3a.secret.key", "minioadmin") // MinIO secret key
                .set("spark.hadoop.fs.s3a.path.style.access", "true")
                .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .set("spark.sql.adaptive.enabled", "true")
                .set("spark.sql.adaptive.coalescePartitions.enabled", "true");

        return SparkSession.builder()
                .config(conf)
                .getOrCreate();
    }

    public static void stopSparkSession(SparkSession spark) {
        if (spark != null) {
            spark.stop();
        }
    }
}
