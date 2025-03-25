package com.example.kafka.aggregator;

import com.example.kafka.model.MergedRecord;
import com.example.kafka.model.Topic1Record;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.Aggregator;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class Topic1Aggregator implements Aggregator<String, Topic1Record, MergedRecord> {

    @Override
    public MergedRecord apply(String key, Topic1Record value, MergedRecord aggregate) {
        // Set fields from Topic1Record
        aggregate.setName(value.getName());
        aggregate.setAge(value.getAge());
        aggregate.setNationality(value.getNationality());
        aggregate.setStudentId(value.getStudentId());
        return aggregate;
    }
}
