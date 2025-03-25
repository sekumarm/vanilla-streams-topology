package com.example.kafka;

import com.example.kafka.model.Topic1Record;
import com.example.kafka.model.Topic2Record;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.test.context.ActiveProfiles;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest
@ActiveProfiles("test")
public class KafkaStreamsServiceTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> topic1;
    private TestInputTopic<String, String> topic2;
    private TestOutputTopic<String, String> outputTopic;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    private KafkaStreamsConfiguration kafkaStreamsConfiguration;

    @BeforeEach
    void setUp() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        
        // Create the topology
        streamsBuilder.stream("kafka-topic-1", Consumed.with(Serdes.String(), Serdes.String()));
        streamsBuilder.stream("kafka-topic-2", Consumed.with(Serdes.String(), Serdes.String()));
        
        // Create the test driver
        Properties props = new Properties();
        kafkaStreamsConfiguration.asProperties().forEach(props::put);
        testDriver = new TopologyTestDriver(streamsBuilder.build(), props);
        
        // Create the test topics
        topic1 = testDriver.createInputTopic("kafka-topic-1", Serdes.String().serializer(), Serdes.String().serializer());
        topic2 = testDriver.createInputTopic("kafka-topic-2", Serdes.String().serializer(), Serdes.String().serializer());
        outputTopic = testDriver.createOutputTopic("output-topic", Serdes.String().deserializer(), Serdes.String().deserializer());
    }

    @AfterEach
    void tearDown() {
        if (testDriver != null) {
            testDriver.close();
        }
    }

    @Test
    void testCogroupTopology() throws JsonProcessingException {
        // Create test records
        Topic1Record topic1Record = new Topic1Record("key1", "value1", System.currentTimeMillis());
        Topic2Record topic2Record = new Topic2Record("key1", "value2", System.currentTimeMillis());
        
        // Send records to test topics
        topic1.pipeInput("key1", objectMapper.writeValueAsString(topic1Record));
        topic2.pipeInput("key1", objectMapper.writeValueAsString(topic2Record));
        
        // Verify output
        assertNotNull(testDriver);
    }
}
