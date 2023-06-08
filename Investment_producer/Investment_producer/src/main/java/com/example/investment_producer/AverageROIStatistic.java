package com.example.investment_producer;

import com.example.investment_consumer.ROIDto;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.List;
import java.util.stream.StreamSupport;

@Builder
public class AverageROIStatistic implements ROIStatistic, Serializable {

    private final SinkFunction<String> sink;

    private final double refValue;

    private final double valueExceeded;

    private final int sizeOfWindow;

    private final int windowSlide;

    @Override
    public void calculate(DataStream<ROIDto> rois) {
        rois.map(ROIDto::getSample)
                .countWindowAll(sizeOfWindow,windowSlide)
                .apply(AverageAllWindowFunction.of(sizeOfWindow))
                .filter(ReferencesFilter.of(refValue, valueExceeded))
                .map(new ParseToExcelMapper())
                .addSink(sink);
    }

    @RequiredArgsConstructor(staticName = "of")
    static class AverageAllWindowFunction implements AllWindowFunction<Double, Double, GlobalWindow> {

        private final int sizeOfWindow;

        @Override
        public void apply(GlobalWindow window, Iterable<Double> values, Collector<Double> out) {
            List<Double> samples = StreamSupport.stream(values.spliterator(), false).toList();
            if (samples.size() == sizeOfWindow) {
                Double average = samples.stream().mapToDouble(sample -> sample).average().getAsDouble();
                out.collect(average);
            }
        }
    }
}
