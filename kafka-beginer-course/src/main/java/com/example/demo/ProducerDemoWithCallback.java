/*
 * package com.example.demo;
 * 
 * import java.util.Properties;
 * 
 * import org.apache.kafka.clients.producer.Callback; import
 * org.apache.kafka.clients.producer.KafkaProducer; import
 * org.apache.kafka.clients.producer.ProducerConfig; import
 * org.apache.kafka.clients.producer.ProducerRecord; import
 * org.apache.kafka.clients.producer.RecordMetadata; import
 * org.apache.kafka.common.serialization.StringSerializer; import
 * org.slf4j.Logger; import org.slf4j.LoggerFactory; import
 * org.springframework.boot.SpringApplication; import
 * org.springframework.boot.autoconfigure.SpringBootApplication;
 * 
 * @SpringBootApplication public class ProducerDemoWithCallback {
 * 
 * static Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
 * 
 * public static void main(String[] args) {
 * SpringApplication.run(ProducerDemoWithCallback.class, args);
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
 * for (int i = 0; i < 10; i++) {
 * 
 * KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
 * 
 * String value = "Hello World Java " + i;
 * 
 * ProducerRecord<String, String> producerRecord = new
 * ProducerRecord<>("first_topic", "_id" + i, value);
 * 
 * // then send data
 * 
 * producer.send(producerRecord, new Callback() {
 * 
 * @Override public void onCompletion(RecordMetadata metadata, Exception
 * exception) { if (exception == null) {
 * log.info("Record Inserted successfully");
 * 
 * } else { log.info("Record Insertion Failed"); }
 * 
 * } });
 * 
 * producer.flush(); producer.close();
 * 
 * }
 * 
 * }
 * 
 * }
 */