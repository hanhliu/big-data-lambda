package org.example.producerspringboot;

import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.InputStreamReader;

@Service
public class CsvReaderService {
    private final KafkaProducerService kafkaProducerService;

    public CsvReaderService(KafkaProducerService kafkaProducerService) {
        this.kafkaProducerService = kafkaProducerService;
    }

    public void readAndSendCsv(String topic) throws Exception {
        ClassPathResource resource = new ClassPathResource("data/sensor.csv");
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(resource.getInputStream()))) {

            String line;
            while ((line = reader.readLine()) != null) {
                kafkaProducerService.sendMessage(topic, line);
                Thread.sleep(5000); // gửi mỗi 5 giây
            }
        }
    }
}