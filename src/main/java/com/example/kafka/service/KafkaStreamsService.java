package com.example.kafka.service;

import com.example.kafka.model.MergedRecord;
import com.example.kafka.model.Topic1Record;
import com.example.kafka.model.Topic2Record;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;
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
        
        // Define the initializer for the MergedRecord
        Initializer<MergedRecord> initializer = () -> 
            MergedRecord.builder()
                .topic1Records(new ArrayList<>())
                .topic2Records(new ArrayList<>())
                .build();
        
        // Define the aggregator for topic1
        Aggregator<String, String, MergedRecord> topic1Aggregator = (key, value, aggregate) -> {
            try {
                Topic1Record record = objectMapper.readValue(value, Topic1Record.class);
                aggregate.getTopic1Records().add(record);
                return aggregate;
            } catch (JsonProcessingException e) {
                log.error("Error deserializing topic1 record: {}", e.getMessage());
                return aggregate;
            }
        };
        
        // Define the aggregator for topic2
        Aggregator<String, String, MergedRecord> topic2Aggregator = (key, value, aggregate) -> {
            try {
                Topic2Record record = objectMapper.readValue(value, Topic2Record.class);
                aggregate.getTopic2Records().add(record);
                return aggregate;
            } catch (JsonProcessingException e) {
                log.error("Error deserializing topic2 record: {}", e.getMessage());
                return aggregate;
            }
        };
        
        // Create a CogroupedKStream using the cogroup API
        CogroupedKStream<String, MergedRecord> cogroupedStream = topic1GroupedStream
                .cogroup(topic1Aggregator)
                .cogroup(topic2GroupedStream, topic2Aggregator);
        
        // Create a materialized view for the aggregation
        Materialized<String, MergedRecord, ?> materialized = Materialized
                .<String, MergedRecord>as(Stores.inMemoryKeyValueStore("cogrouped-store"))
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.serdeFrom(
                        (topic, data) -> {
                            try {
                                return objectMapper.writeValueAsBytes(data);
                            } catch (JsonProcessingException e) {
                                log.error("Error serializing MergedRecord: {}", e.getMessage());
                                return new byte[0];
                            }
                        },
                        (topic, data) -> {
                            try {
                                return objectMapper.readValue(data, MergedRecord.class);
                            } catch (JsonProcessingException e) {
                                log.error("Error deserializing MergedRecord: {}", e.getMessage());
                                return MergedRecord.builder().build();
                            }
                        }
                ));
        
        // Aggregate the cogrouped streams with a time window
        KTable<Windowed<String>, MergedRecord> mergedTable = cogroupedStream
                .windowedBy(TimeWindows.of(Duration.ofMinutes(5)))
                .aggregate(
                        initializer,
                        materialized
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
