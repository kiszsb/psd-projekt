package com.example.investment_producer;

import com.example.investment_consumer.ROIDto;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;


@Component
public class StatisticCalculatorController implements ApplicationListener<ContextRefreshedEvent> {

    private final ImmutablePair<Map<String, DataStream<ROIDto>>,
            StreamExecutionEnvironment> flinkToKafkaStream;

    private final StatisticCalculatorBuilder statisticCalculatorBuilder;

    public StatisticCalculatorController(
            @Qualifier("flinkKafkaDataStream") ImmutablePair<Map<String, DataStream<ROIDto>>,
                    StreamExecutionEnvironment> flinkToKafkaStream,
            StatisticCalculatorBuilder statisticCalculatorBuilder) {
        this.flinkToKafkaStream = flinkToKafkaStream;
        this.statisticCalculatorBuilder = statisticCalculatorBuilder;
    }

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        try {
            Map<String, DataStream<ROIDto>> rois = flinkToKafkaStream.getKey();
            for (Map.Entry<String, DataStream<ROIDto>> entry : rois.entrySet()) {
                String key = entry.getKey();
                DataStream<ROIDto> value = entry.getValue();
                List<ROIStatistic> statistics = statisticCalculatorBuilder.buildInvestmentStatisticsWithName(key);
                for (ROIStatistic statistic : statistics) {
                    statistic.calculate(value);
                }
            }

            StreamExecutionEnvironment environment = flinkToKafkaStream.getValue();
            environment.execute("Context");
        } catch (Exception e) {}
    }
}
