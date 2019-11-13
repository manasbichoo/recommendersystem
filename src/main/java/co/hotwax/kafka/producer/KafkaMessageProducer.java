package co.hotwax.kafka.producer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import co.hotwax.kafka.KafkaMessage;
import co.hotwax.message.MessageProducer;

/**
 * 
 * @author grv
 *
 */
public class KafkaMessageProducer implements MessageProducer {

	private static org.apache.kafka.clients.producer.KafkaProducer<Object, Object> KafkaProducerInstance;

	private static KafkaMessageProducer instance;

	private KafkaMessageProducer() {
	}

	public static KafkaMessageProducer getInstance() {
		if (instance == null) {
			instance = new KafkaMessageProducer();
			init();
		}
		return instance;
	}

	private static void init() {
		Properties properties = new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		KafkaProducerInstance = new KafkaProducer<>(properties);
	}

	@Override
	public boolean produce(KafkaMessage message) {
		// TODO Auto-generated method stub

		final ProducerRecord<Object, Object> record = new ProducerRecord<>(message.getToTopic(), message.getKey(),
				message.getValue());
		long time = System.currentTimeMillis();
		RecordMetadata metadata = null;
		try {
			metadata = KafkaProducerInstance.send(record).get();
		} catch (InterruptedException | ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}

		long elapsedTime = System.currentTimeMillis() - time;
		System.out.printf("sent record(key=%s value=%s) " + "meta(partition=%d, offset=%d) time=%d\n", record.key(),
				record.value(), metadata.partition(), metadata.offset(), elapsedTime);

		return true;
	}
}
