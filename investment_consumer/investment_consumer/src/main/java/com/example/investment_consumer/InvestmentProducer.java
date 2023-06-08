package com.example.investment_consumer;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class InvestmentProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    private final ROIGenerator ROIGenerator;

    public void publishReturnOnInvestment() {
        ROIDto sample = ROIDto.builder()
                .sample(ROIGenerator.generate())
                .build();
        kafkaTemplate.send("Inwestycja_A", sample);
    }
}
