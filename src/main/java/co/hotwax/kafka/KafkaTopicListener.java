package co.hotwax.kafka;

import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Kafka Topic Listener
 * @author grv
 *
 */
public class KafkaTopicListener {

	public static void main(String[] args) {

		KafkaConsumer<String, String> consumer = createConsumer();
		consumer.subscribe(Arrays.asList("consumertest"));
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE); // Immediately Pull a record if
																						// available
			for (ConsumerRecord<String, String> record : records) {
				System.out.println("Data recieved : " + record.value());
			}
		}
	}

	private static KafkaConsumer<String, String> createConsumer() {

		Properties properties = new Properties();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, "ofbiz.dashboard");
		properties.put(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		return new KafkaConsumer<>(properties);
	}

}
