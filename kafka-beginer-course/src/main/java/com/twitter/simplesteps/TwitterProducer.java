package com.twitter.simplesteps;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

@SpringBootApplication
public class TwitterProducer {

	List<String> terms = Lists.newArrayList("kafka");

	public static void main(String[] args) {

		SpringApplication.run(TwitterProducer.class, args);

		new TwitterProducer().run();

	}

	public void run() {

		/**
		 * Set up your blocking queues: Be sure to size these properly based on expected
		 * TPS of your stream
		 */
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

		// create twitter client

		Client client = createTwitterClient(msgQueue);
		client.connect();

		// create a kafka producer

		KafkaProducer<String, String> producer = createKafkaProducer();

		// add a shutdown hook

		/*
		 * Runtime.getRuntime().addShutdownHook(new Thread(() -> { {
		 * System.out.println("application info"); client.stop();
		 * System.out.println("Closing producer"); producer.close();
		 * System.out.println("Done ...."); } }));
		 */

		// loop to send tweets to kafka

		// on a different thread, or multiple different threads....
		while (!client.isDone()) {
			String msg = null;
			try {
				msg = msgQueue.poll(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				e.printStackTrace();
				client.stop();
			}
			if (msg != null) {

				System.out.println(msg);
				producer.send(new ProducerRecord<String, String>("twitter_tweets", null, msg), new Callback() {

					@Override
					public void onCompletion(RecordMetadata metadata, Exception exception) {

						if (exception != null) {
							System.out.println("Something bad happend");
						}

					}
				});

			}

		}
		System.out.println("End of the application");
	}

	public Client createTwitterClient(BlockingQueue<String> msgQueue) {

		String consumerKey = "mSH3RiFB8g7soQqEh3U4zpOEf";
		String consumerSecretKey = "7fOEn4MhnF870qGVRktSBucRG3ab2wkAlOcikwHoK0pwOaU68A";
		String accessToken = "846970061571153921-6yHnWW9RoCk4uJiKIQQy8FlBJC7ATLc";
		String tokenSecret = "lz3xsBHk1X5BF6nngaUQG4ROPex4LPxoM8l9YMwEE2NUR";
		/**
		 * Set up your blocking queues: Be sure to size these properly based on expected
		 * TPS of your stream
		 */

		/**
		 * Declare the host you want to connect to, the endpoint, and authentication
		 * (basic auth or oauth)
		 */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		// Optional: set up some followings and track terms

		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecretKey, accessToken, tokenSecret);

		ClientBuilder builder = new ClientBuilder().name("Hosebird-Client-01") // optional: mainly for the logs
				.hosts(hosebirdHosts).authentication(hosebirdAuth).endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));
		// optional: use this if you want to process client events

		Client hosebirdClient = builder.build();
		// Attempts to establish a connection.
		return hosebirdClient;

	}

	public KafkaProducer<String, String> createKafkaProducer() {
		// create produce properties

		Properties properties = new Properties();

		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		/*
		 * // create safe producer
		 * 
		 * properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		 * properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
		 * properties.setProperty(ProducerConfig.RETRIES_CONFIG,
		 * Integer.toString(Integer.MAX_VALUE));
		 * properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
		 * "5"); // kafka 2.0 >= 1.1 so we can
		 */	// keep this as 5. use 1
																							// otherwise

		// High throughtput producer (at the expense of a bit latency and cpu usage)

		/*
		 * properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		 * properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
		 * properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,
		 * Integer.toString(32*1024)); // 32 kb batch size
		 */
		// create producer

		return new KafkaProducer<>(properties);
	}

}
