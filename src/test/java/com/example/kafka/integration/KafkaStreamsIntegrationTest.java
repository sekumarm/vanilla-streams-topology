package com.example.kafka.integration;

import com.example.kafka.model.MergedRecord;
import com.example.kafka.model.Topic1Record;
import com.example.kafka.model.Topic2Record;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@DirtiesContext
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 1, topics = {"kafka-topic-1", "kafka-topic-2", "output-topic"})
public class KafkaStreamsIntegrationTest {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    private Producer<String, String> producer;
    private Consumer<String, String> consumer;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        // Configure producer
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafkaBroker);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        DefaultKafkaProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(producerProps);
        producer = pf.createProducer();

        // Configure consumer
        Map<String, Object> consumerProps = new HashMap<>(KafkaTestUtils.consumerProps("test-group", "true", embeddedKafkaBroker));
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
        consumer = cf.createConsumer();
        consumer.subscribe(Collections.singletonList("output-topic"));
    }

    @AfterEach
    void tearDown() {
        if (producer != null) {
            producer.close();
        }
        if (consumer != null) {
            consumer.close();
        }
    }

    @Test
    void testValidRecordsFromBothTopics() throws JsonProcessingException {
        // Create test records
        Topic1Record topic1Record = new Topic1Record("John Doe", 20, "US", 12345);
        Topic2Record topic2Record = new Topic2Record("Computer Science", true, 12345, "MIT");

        // Send records to test topics
        producer.send(new ProducerRecord<>("kafka-topic-1", String.valueOf(topic1Record.getStudentId()), objectMapper.writeValueAsString(topic1Record)));
        producer.send(new ProducerRecord<>("kafka-topic-2", topic1Record.getName(), objectMapper.writeValueAsString(topic2Record)));
        producer.flush();

        // Wait for processing
        ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(10));

        // Verify output
        assertFalse(records.isEmpty(), "Should have received records");
        
        // Parse and validate the merged record
        String outputValue = records.iterator().next().value();
        MergedRecord mergedRecord = objectMapper.readValue(outputValue, MergedRecord.class);
        
        assertEquals("John Doe", mergedRecord.getName());
        assertEquals(20, mergedRecord.getAge());
        assertEquals("US", mergedRecord.getNationality());
        assertEquals(12345, mergedRecord.getStudentId());
        assertEquals("Computer Science", mergedRecord.getDegree());
        assertTrue(mergedRecord.isAuthorized());
        assertEquals("MIT", mergedRecord.getCollegeName());
    }

    @Test
    void testPoisonMessage() throws JsonProcessingException {
        // Create valid record for topic1
        Topic1Record topic1Record = new Topic1Record("Jane Smith", 22, "UK", 67890);
        
        // Send valid record to topic1
        producer.send(new ProducerRecord<>("kafka-topic-1", String.valueOf(topic1Record.getStudentId()), objectMapper.writeValueAsString(topic1Record)));
        
        // Send poison message (invalid JSON) to topic2
        producer.send(new ProducerRecord<>("kafka-topic-2", "Jane Smith", "This is not valid JSON"));
        producer.flush();
        
        // Wait for processing
        ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(10));
        
        // Verify that the poison message was filtered out and didn't crash the stream
        // The valid record from topic1 should still be processed
        assertFalse(records.isEmpty(), "Should have received records from valid topic1 message");
        
        // Parse and validate the merged record
        String outputValue = records.iterator().next().value();
        MergedRecord mergedRecord = objectMapper.readValue(outputValue, MergedRecord.class);
        
        // Only topic1 fields should be populated
        assertEquals("Jane Smith", mergedRecord.getName());
        assertEquals(22, mergedRecord.getAge());
        assertEquals("UK", mergedRecord.getNationality());
        assertEquals(67890, mergedRecord.getStudentId());
        
        // Topic2 fields should have default values
        assertNull(mergedRecord.getDegree());
        assertFalse(mergedRecord.isAuthorized());
        assertNull(mergedRecord.getCollegeName());
    }
}
