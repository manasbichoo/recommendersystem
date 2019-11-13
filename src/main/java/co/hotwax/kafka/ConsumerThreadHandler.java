package co.hotwax.kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import co.hotwax.ml.clustering.ProductCategoryClustering;

/**
 * 
 * @author grv
 *
 */
public class ConsumerThreadHandler implements Runnable {

	private JSONObject kafkaRecord;

	public ConsumerThreadHandler(JSONObject consumerRecord) {
		this.kafkaRecord = consumerRecord;
	}

	public void run() {
		// System.out.println("Process: " + consumerRecord.value() + ", Offset: " +
		// consumerRecord.offset()
		// + ", By ThreadID: " + Thread.currentThread().getId());

		try {
			try {
				processRecord(kafkaRecord);
			} catch (InterruptedException | ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void processRecord(JSONObject consumerRecord)
			throws JSONException, InterruptedException, ExecutionException {

		String category = (String) consumerRecord.get("category");
		String replyTopic = (String) consumerRecord.get("reply-topic");
		String correlationId = (String) consumerRecord.get("correlation-id");
		System.out.println("Running spark for " + category + "With reply topic " + replyTopic + ", and correlation id "
				+ correlationId);
		Dataset<Row> categoryDs = ProductCategoryClustering.getCategoryPriceRange(category);

		String jsonResponse = ((String[]) categoryDs.toJSON().collect())[0];

		Map<String, String> responseMap = new HashMap<String, String>();
		responseMap.put("correlation-id", correlationId);
		responseMap.put("spark-response", jsonResponse);

		sendResponse(new JSONObject(responseMap).toString(), replyTopic, correlationId);

	}

	private void sendResponse(String jsonResponse, String replyTopic, String correlationId)
			throws InterruptedException, ExecutionException {

		System.out.println("jsonResponse for ofbiz " + jsonResponse);
		System.out.println("replyTopic " + replyTopic);
		System.out.println("correlationId " + correlationId);

		final ProducerRecord<String, String> record = new ProducerRecord<>(replyTopic, "spark-response", jsonResponse);
		Producer<String, String> producer = createProducer();
		RecordMetadata metadata = producer.send(record).get();
		System.out.printf("sent record(key=%s value=%s) " + "meta(partition=%d, offset=%d)", record.key(),
				record.value(), metadata.partition(), metadata.offset());
		producer.close();

	}

	private static Producer<String, String> createProducer() {

		Properties properties = new Properties();

		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.put(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		return new KafkaProducer<>(properties);
	}
}