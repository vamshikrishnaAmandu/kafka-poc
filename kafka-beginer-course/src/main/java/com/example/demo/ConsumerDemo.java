package com.example.demo;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ConsumerDemo {
	
	static Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getName());

	public static void main(String[] args) {

		SpringApplication.run(ConsumerDemo.class, args);
		
		String bootstrapServer="127.0.0.1:9092";
		String groupId="my-fourth-application";

		

		Properties properties = new Properties();
		
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest" );
		
		// Create a Consumer
		
		KafkaConsumer<String, String> consumer=new KafkaConsumer<>(properties);
		
		// Subscribe consumer to our topic
		
		consumer.subscribe(Arrays.asList("first_topic"));
		
		//poll for new data
		
		while(true) {
		ConsumerRecords<String, String>	 consumerRecords=consumer.poll(Duration.ofMillis(100));
		
		for(ConsumerRecord<String , String> record: consumerRecords) {
			
			
			log.info("Value ::"+record.value());
			log.info("Partition::"+record.partition());
			
			
		}
		}
		
	}

}
