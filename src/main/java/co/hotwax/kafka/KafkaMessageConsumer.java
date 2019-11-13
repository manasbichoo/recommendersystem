package co.hotwax.kafka;

import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import co.hotwax.ml.clustering.ProductCategoryClustering;

public class KafkaMessageConsumer implements Runnable {

	public static final String REQUEST_TOPIC_NAME = "product_category_range";

	protected final String responseTopic;
	protected final KafkaConsumer<String, String> kafkaConsumer;
	private final ExecutorService executor;
	private final int numberOfThreads = 10;

	private CountDownLatch shutdownLatch = new CountDownLatch(1);

	public KafkaMessageConsumer(String topicName) {
		this.responseTopic = topicName;

		final Properties properties = new Properties();
		properties.put("bootstrap.servers", System.getProperty("kafka.bootstrap.servers", "localhost:9092"));
		properties.put("group.id", topicName);
		properties.put("enable.auto.commit", "true");
		properties.put("auto.commit.interval.ms", "100");
		properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		kafkaConsumer = new KafkaConsumer<>(properties);
		kafkaConsumer.subscribe(Arrays.asList(responseTopic));

		executor = new ThreadPoolExecutor(numberOfThreads, numberOfThreads, 0L, TimeUnit.MILLISECONDS,
				new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
	}

	@Override
	public void run() {
		System.out.println("Started listeing for Kafka messages ... ");
		while (!Thread.currentThread().isInterrupted()) {
			final ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Long.MAX_VALUE);
			for (ConsumerRecord<String, String> record : consumerRecords.records(responseTopic)) {
				System.out.println("[Consumer]Received message with key: " + record.key());
				

				try {
					executor.submit(new ConsumerThreadHandler(new JSONObject(record.value())));
				} catch (JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				shutdownLatch.countDown();
				kafkaConsumer.commitAsync();

			}
		}

	}

	public void close() {
		try {
			kafkaConsumer.close();
			shutdownLatch.await();
			if (executor != null) {
				executor.shutdown();
			}
		} catch (InterruptedException e) {
			// logger.error("Error", e);
			e.printStackTrace();
		}
	}

}
