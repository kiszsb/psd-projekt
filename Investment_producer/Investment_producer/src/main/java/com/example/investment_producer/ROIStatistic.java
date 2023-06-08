package com.example.investment_producer;

import com.example.investment_consumer.ROIDto;
import org.apache.flink.streaming.api.datastream.DataStream;

public interface ROIStatistic {

    void calculate(DataStream<ROIDto> investments);
}
