package com.example.kafka.service;

import com.example.kafka.aggregator.Topic1Aggregator;
import com.example.kafka.aggregator.Topic2Aggregator;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class KafkaStreamsService {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Topic1Aggregator topic1Aggregator;
    private final Topic2Aggregator topic2Aggregator;

    @Autowired
    public void buildPipeline(StreamsBuilder streamsBuilder) {
        // Create KStream for topic1 - Key is already studentId
        KStream<String, String> topic1Stream = streamsBuilder.stream("kafka-topic-1");
        
        // Create KStream for topic2 - Key is studentName
        KStream<String, String> topic2Stream = streamsBuilder.stream("kafka-topic-2");
        
        // Deserialize topic1 records
        KStream<String, Topic1Record> topic1RecordStream = topic1Stream.mapValues(value -> {
            try {
                return objectMapper.readValue(value, Topic1Record.class);
            } catch (JsonProcessingException e) {
                log.error("Error deserializing topic1 record: {}", e.getMessage());
                return null;
            }
        }).filter((key, value) -> value != null);
        
        // Deserialize topic2 records
        KStream<String, Topic2Record> topic2RecordStream = topic2Stream.mapValues(value -> {
            try {
                return objectMapper.readValue(value, Topic2Record.class);
            } catch (JsonProcessingException e) {
                log.error("Error deserializing topic2 record: {}", e.getMessage());
                return null;
            }
        }).filter((key, value) -> value != null);
        
        // Repartition topic2 to have studentId as key
        KStream<String, Topic2Record> topic2RepartitionedStream = topic2RecordStream
                .selectKey((key, value) -> String.valueOf(value.getStudentId()));
        
        // Group topic1 stream by studentId key
        KGroupedStream<String, Topic1Record> topic1GroupedStream = topic1RecordStream.groupByKey();
        
        // Group repartitioned topic2 stream by studentId key
        KGroupedStream<String, Topic2Record> topic2GroupedStream = topic2RepartitionedStream.groupByKey();
        
        // Combined cogroup and mergedTable creation block
        KTable<String, MergedRecord> mergedTable = topic1GroupedStream
                .cogroup(topic1Aggregator)
                .cogroup(topic2GroupedStream, topic2Aggregator)
                .aggregate(
                        () -> MergedRecord.builder().build(),
                        Named.as("cogrouped-aggregation")
                );
        
        // Create and send output stream
        createAndSendOutputStream(mergedTable);
        
        log.info("Kafka Streams topology built successfully");
    }
    
    /**
     * Creates an output stream from the merged table and sends it to the output topic
     * 
     * @param mergedTable The KTable containing merged records
     */
    private void createAndSendOutputStream(KTable<String, MergedRecord> mergedTable) {
        // Convert the KTable to a KStream
        KStream<String, String> outputStream = mergedTable
                .toStream()
                .map((key, value) -> {
                    try {
                        return KeyValue.pair(key, objectMapper.writeValueAsString(value));
                    } catch (JsonProcessingException e) {
                        log.error("Error serializing merged record: {}", e.getMessage());
                        return KeyValue.pair(key, "Error processing record");
                    }
                });
        
        // Send the result to the output topic
        outputStream.to("output-topic");
        
        log.info("Output stream created and sent to output-topic");
    }
}
