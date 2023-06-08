package com.example.investment_producer;

import com.example.investment_consumer.ROIDto;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class ROISchema implements DeserializationSchema<ROIDto> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public ROIDto deserialize(byte[] message) throws IOException {
        return objectMapper.readValue(message, ROIDto.class);
    }

    @Override
    public boolean isEndOfStream(ROIDto nextElement) {
        return false;
    }

    @Override
    public TypeInformation<ROIDto> getProducedType() {
        return TypeExtractor.getForClass(ROIDto.class);
    }
}
