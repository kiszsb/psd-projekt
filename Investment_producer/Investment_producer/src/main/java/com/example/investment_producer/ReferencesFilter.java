package com.example.investment_producer;

import lombok.RequiredArgsConstructor;
import org.apache.flink.api.common.functions.FilterFunction;

@RequiredArgsConstructor(staticName = "of")
public class ReferencesFilter implements FilterFunction<Double> {

    private final double valueExceeded;

    private final double refValue;

    @Override
    public boolean filter(Double statistic) {
        return (((refValue - statistic) / (1 + refValue)) >= valueExceeded);
    }
}
