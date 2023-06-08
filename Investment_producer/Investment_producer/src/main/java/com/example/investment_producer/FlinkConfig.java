package com.example.investment_producer;

import com.example.investment_consumer.ROIDto;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Configuration
public class FlinkConfig {

    @Bean(name = "flinkKafkaDataStream")
    public ImmutablePair<Map<String, DataStream<ROIDto>>, StreamExecutionEnvironment> flinkKafkaDataStream() {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.enableCheckpointing(10000);

        List<String> topicNames = List.of("Inwestycja_A", "Inwestycja_B",
                "Inwestycja_C", "Inwestycja_D", "Inwestycja_E");
        Map<String, DataStream<ROIDto>> dataStreams = new HashMap<>();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "consumerGroup");
        for (String topicName : topicNames) {
            dataStreams.put(topicName, getExecutionEnvironment(properties, environment, topicName));
        }

        return new ImmutablePair<>(dataStreams, environment);
    }

    @Bean
    StreamExecutionEnvironment streamExecutionEnvironment() {
        return StreamExecutionEnvironment.getExecutionEnvironment();
    }

    private static DataStream<ROIDto> getExecutionEnvironment(Properties properties, StreamExecutionEnvironment environment, String topicName) {
        return environment.addSource(new FlinkKafkaConsumer<>(topicName, new ROISchema(), properties));
    }

}
