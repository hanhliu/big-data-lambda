package org.example.producerspringboot;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ProducerSpringbootApplication implements CommandLineRunner {

    private final CsvReaderService csvReaderService;

    @Value("${app.kafka.topic}")
    private String topic;

    public ProducerSpringbootApplication(CsvReaderService csvReaderService) {
        this.csvReaderService = csvReaderService;
    }

    public static void main(String[] args) {
        SpringApplication.run(ProducerSpringbootApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        csvReaderService.readAndSendCsv(topic);
    }
}
