package com.example.kafka.aggregator;

import com.example.kafka.model.MergedRecord;
import com.example.kafka.model.Topic2Record;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.Aggregator;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class Topic2Aggregator implements Aggregator<String, Topic2Record, MergedRecord> {

    @Override
    public MergedRecord apply(String key, Topic2Record value, MergedRecord aggregate) {
        // Set fields from Topic2Record
        aggregate.setDegree(value.getDegree());
        aggregate.setAuthorized(value.isAuthorized());
        aggregate.setCollegeName(value.getCollegeName());
        return aggregate;
    }
}
