package com.example.investment_producer;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class StatisticCalculatorBuilder {

    public List<ROIStatistic> buildInvestmentStatisticsWithName(String investmentName) {
        AverageOfSmallestROIStatistic averageStatistic = AverageOfSmallestROIStatistic.builder()
                .percentageOfSmallestStatistics(10)
                .refValue(0.01)
                .valueExceeded(0.06)
                .sizeOfWindow(30)
                .windowSlide(1)
                .sinkFunction(saveToExcelSink("statystyki/" + investmentName + "/10procentROI"))
                .build();

        QuantileROIStatistic quantileROIStatistic = QuantileROIStatistic.builder()
                .quantileOrder(0.1)
                .refValue(-0.05)
                .valueExceeded(0.01)
                .sizeOfWindow(30)
                .windowSlide(1)
                .sinkFunction(saveToExcelSink("statystyki/" + investmentName + "/kwantylROI"))
                .build();

        AverageROIStatistic averageOfSmallestROIStatistic = AverageROIStatistic.builder()
                .refValue(0.01)
                .valueExceeded(0.01)
                .sizeOfWindow(30)
                .windowSlide(1)
                .sink(saveToExcelSink("statystyki/" + investmentName + "/sredniaROI"))
                .build();

        return List.of(averageStatistic, quantileROIStatistic, averageOfSmallestROIStatistic);
    }

    private StreamingFileSink<String> saveToExcelSink(String path) {
        return StreamingFileSink.forRowFormat(new Path(path), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(DefaultRollingPolicy.builder().withMaxPartSize(3000).build()).build();
    }
}
