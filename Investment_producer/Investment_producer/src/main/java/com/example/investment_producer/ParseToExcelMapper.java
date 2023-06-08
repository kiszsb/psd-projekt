package com.example.investment_producer;

import org.apache.flink.api.common.functions.MapFunction;

import java.text.SimpleDateFormat;
import java.util.Date;

public class ParseToExcelMapper implements MapFunction<Double, String> {

    @Override
    public String map(Double value) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
        Date date = new Date();
        String timestamp = dateFormat.format(date);
        return value.toString() + ";" + timestamp;
    }
}
