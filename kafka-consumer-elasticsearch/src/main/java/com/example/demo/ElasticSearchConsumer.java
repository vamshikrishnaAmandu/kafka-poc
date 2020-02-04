package com.example.demo;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ElasticSearchConsumer {

	public static RestHighLevelClient createClient() {

		String hostName = "kafka-course-4182548393.ap-southeast-2.bonsaisearch.net";
		String userName = "najjgn5bv5";
		String password = "uk3nzjwohl";

		// don't do if you run a local elastic search

		final CredentialsProvider credentialProvider = new BasicCredentialsProvider();

		credentialProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, password));

		RestClientBuilder builder = RestClient.builder(new HttpHost(hostName, 443, "https"))
				.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {

					@Override
					public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
						return httpClientBuilder.setDefaultCredentialsProvider(credentialProvider);
					}
				});

		return new RestHighLevelClient(builder);
	}

	private static KafkaConsumer<String, String> createKafkaConsumer() {

		String bootstrapServer = "127.0.0.1:9092";
		String groupId = "kafka-demo-elasticsearch";
		String topic = "twitter_tweets";

		Properties properties = new Properties();

		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		// Create a Consumer

		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

		consumer.subscribe(Arrays.asList(topic));
		return consumer;

	}

	public static void main(String[] args) throws IOException, InterruptedException {
		SpringApplication.run(ElasticSearchConsumer.class, args);
		RestHighLevelClient client = createClient();
		
		
		
		KafkaConsumer<String, String>  consumer= createKafkaConsumer();
		
		while(true) {
			ConsumerRecords<String, String>	 consumerRecords=consumer.poll(Duration.ofMillis(100));
			
			for(ConsumerRecord<String , String> record: consumerRecords) {
				
				
			//  Where we insert data into elastic search
				
				String jsonString = record.value();
				IndexRequest indexRequest = new IndexRequest("twitter", "tweets").source(jsonString, XContentType.JSON);
				IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
				String id = indexResponse.getId();
				System.out.println("Id :" + id);
				Thread.sleep(1000);
				
			}
			
			}
		//client.close();
		}
		
	}


