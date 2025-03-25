package com.example.kafka.service;

import com.example.kafka.model.MergedRecord;
import com.example.kafka.model.Topic1Record;
import com.example.kafka.model.Topic2Record;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@Service
@Slf4j
@RequiredArgsConstructor
public class KafkaStreamsService {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    public void buildPipeline(StreamsBuilder streamsBuilder) {
        // Create KStream for topic1
        KStream<String, String> topic1Stream = streamsBuilder.stream("kafka-topic-1");
        
        // Create KStream for topic2
        KStream<String, String> topic2Stream = streamsBuilder.stream("kafka-topic-2");
        
        // Group topic1 stream by key
        KGroupedStream<String, String> topic1GroupedStream = topic1Stream.groupByKey();
        
        // Group topic2 stream by key
        KGroupedStream<String, String> topic2GroupedStream = topic2Stream.groupByKey();
        
        // Create a CogroupedKStream using the cogroup API
        CogroupedKStream<String, String> cogroupedStream = topic1GroupedStream.cogroup(
                (key, value, aggregate) -> {
                    try {
                        Topic1Record record = objectMapper.readValue(value, Topic1Record.class);
                        if (aggregate.topic1Records == null) {
                            aggregate.topic1Records = new ArrayList<>();
                        }
                        aggregate.topic1Records.add(record);
                        return aggregate;
                    } catch (JsonProcessingException e) {
                        log.error("Error deserializing topic1 record: {}", e.getMessage());
                        return aggregate;
                    }
                }
        );
        
        // Add topic2 to the cogroup
        cogroupedStream = cogroupedStream.cogroup(topic2GroupedStream, 
                (key, value, aggregate) -> {
                    try {
                        Topic2Record record = objectMapper.readValue(value, Topic2Record.class);
                        if (aggregate.topic2Records == null) {
                            aggregate.topic2Records = new ArrayList<>();
                        }
                        aggregate.topic2Records.add(record);
                        return aggregate;
                    } catch (JsonProcessingException e) {
                        log.error("Error deserializing topic2 record: {}", e.getMessage());
                        return aggregate;
                    }
                }
        );
        
        // Aggregate the cogrouped streams with a time window
        KTable<Windowed<String>, MergedRecord> mergedTable = cogroupedStream
                .windowedBy(TimeWindows.of(Duration.ofMinutes(5)))
                .aggregate(
                        () -> MergedRecord.builder().topic1Records(new ArrayList<>()).topic2Records(new ArrayList<>()).build(),
                        Named.as("cogrouped-aggregation")
                );
        
        // Convert the KTable to a KStream
        KStream<String, String> outputStream = mergedTable
                .toStream()
                .map((windowedKey, value) -> {
                    value.setId(windowedKey.key());
                    try {
                        return KeyValue.pair(windowedKey.key(), objectMapper.writeValueAsString(value));
                    } catch (JsonProcessingException e) {
                        log.error("Error serializing merged record: {}", e.getMessage());
                        return KeyValue.pair(windowedKey.key(), "Error processing record");
                    }
                });
        
        // Send the result to the output topic
        outputStream.to("output-topic");
        
        log.info("Kafka Streams topology built successfully");
    }
}
