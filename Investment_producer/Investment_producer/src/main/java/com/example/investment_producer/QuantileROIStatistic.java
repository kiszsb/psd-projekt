package com.example.investment_producer;

import com.example.investment_consumer.ROIDto;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.List;
import java.util.stream.StreamSupport;

@Builder
public class QuantileROIStatistic implements ROIStatistic {

    private final SinkFunction<String> sinkFunction;

    private final double quantileOrder;

    private final double refValue;

    private final double valueExceeded;

    private final int sizeOfWindow;

    private final int windowSlide;

    @Override
    public void calculate(DataStream<ROIDto> rois) {
        rois.map(ROIDto::getSample)
                .countWindowAll(sizeOfWindow,windowSlide)
                .apply(QuantileAllWindowFunction.of(quantileOrder, sizeOfWindow))
                .filter(ReferencesFilter.of(refValue, valueExceeded))
                .map(new ParseToExcelMapper())
                .addSink(sinkFunction);
    }

    @RequiredArgsConstructor(staticName = "of")
    static class QuantileAllWindowFunction implements AllWindowFunction<Double, Double, GlobalWindow> {

        private final double order;

        private final int sizeOfWindow;

        @Override
        public void apply(GlobalWindow window, Iterable<Double> values, Collector<Double> out) {
            List<Double> samples = StreamSupport.stream(values.spliterator(), false)
                    .sorted(Comparator.naturalOrder())
                    .toList();
            if (samples.size() == sizeOfWindow) {
                int index = (int) Math.ceil(order * samples.size());
                out.collect(samples.get(index - 1));
            }
        }
    }
}
