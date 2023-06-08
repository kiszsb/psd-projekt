package com.example.investment_producer;

import com.example.investment_consumer.ROIDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

    @KafkaListener(topics = "Inwestycja_A", groupId = "groupId")
    public void consumeInvestmentsFromA(ROIDto dto) {
        String consumerMessage = String.format("Investment_A: %f", dto.getSample());
        logger.info(consumerMessage);
    }

    @KafkaListener(topics = "Inwestycja_B", groupId = "groupId")
    public void consumeInvestmentsFromB(ROIDto dto) {
        String consumerMessage = String.format("Investment_B: %f", dto.getSample());
        logger.info(consumerMessage);
    }

    @KafkaListener(topics = "Inwestycja_C", groupId = "groupId")
    public void consumeInvestmentsFromC(ROIDto dto) {
        String consumerMessage = String.format("Investment_C: %f", dto.getSample());
        logger.info(consumerMessage);
    }

    @KafkaListener(topics = "Inwestycja_D", groupId = "groupId")
    public void consumeInvestmentsFormD(ROIDto dto) {
        String consumerMessage = String.format("Investment_D: %f", dto.getSample());
        logger.info(consumerMessage);
    }

    @KafkaListener(topics = "Inwestycja_E", groupId = "groupId")
    public void consumeInvestmentsFormE(ROIDto dto) {
        String consumerMessage = String.format("Investment_E: %f", dto.getSample());
        logger.info(consumerMessage);
    }
}
