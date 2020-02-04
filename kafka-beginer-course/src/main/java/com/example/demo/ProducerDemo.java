/*
 * package com.example.demo;
 * 
 * import java.util.Properties;
 * 
 * import org.apache.kafka.clients.producer.KafkaProducer; import
 * org.apache.kafka.clients.producer.ProducerConfig; import
 * org.apache.kafka.clients.producer.ProducerRecord; import
 * org.apache.kafka.common.serialization.StringSerializer; import
 * org.springframework.boot.SpringApplication; import
 * org.springframework.boot.autoconfigure.SpringBootApplication;
 * 
 * @SpringBootApplication public class ProducerDemo {
 * 
 * public static void main(String[] args) {
 * SpringApplication.run(ProducerDemo.class, args);
 * 
 * 
 * // create produce properties
 * 
 * Properties properties = new Properties();
 * 
 * properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
 * "127.0.0.1:9092");
 * properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
 * StringSerializer.class.getName());
 * properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
 * StringSerializer.class.getName());
 * 
 * // create producer
 * 
 * KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
 * 
 * 
 * // create a producer record
 * 
 * ProducerRecord<String, String> producerRecord = new
 * ProducerRecord<>("first_topic", "Hello World Java Producer");
 * 
 * // then send data
 * 
 * producer.send(producerRecord);
 * 
 * 
 * 
 * producer.flush();
 * 
 * producer.close(); }
 * 
 * }
 */