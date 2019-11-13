package co.hotwax.spark.streaming;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import co.hotwax.kafka.KafkaMessageConsumer;

public class SparkStreamingListener {

	private static String categoryId;
	private static KafkaMessageConsumer consumer;
	final static ExecutorService exService;

	static {
		exService = Executors.newSingleThreadExecutor();
	}

	private static void testThread() {
		KafkaMessageConsumer consumer = new KafkaMessageConsumer("product_category_range");
		exService.submit(consumer);

		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

			@Override
			public void run() {
				System.out.println("Trying to close consumer thread");
				consumer.close();
			}
		}));
	}

	public void shutdown() {
		if (consumer != null) {
			consumer.close();
		}
		if (exService != null) {
			exService.shutdown();
		}
		try {
			if (!exService.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
				System.out.println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
			}
		} catch (InterruptedException e) {
			System.out.println("Interrupted during shutdown, exiting uncleanly");
		}
	}

	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub
		LogManager.getLogger("org").setLevel(Level.OFF);
		testThread();

		/*
		 * LogManager.getLogger("org").setLevel(Level.ERROR);
		 * 
		 * SparkConf conf = new
		 * SparkConf().setMaster("local[*]").setAppName("NetworkWordCount");
		 * JavaStreamingContext jssc = new JavaStreamingContext(conf,
		 * Durations.seconds(1)); JavaSparkContext jsc = jssc.sparkContext();
		 * System.out.println("jsc " + jsc); SparkSession session =
		 * SparkSession.builder().appName("RecommendationJob").getOrCreate();
		 * System.out.println("Spark Session " + session);
		 * 
		 * Set<String> topicsSet = new
		 * HashSet<>(Arrays.asList("product_category_range")); Map<String, Object>
		 * kafkaParams = new HashMap<>();
		 * kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		 * kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG,
		 * UUID.randomUUID().toString());
		 * kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
		 * StringDeserializer.class);
		 * kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
		 * StringDeserializer.class);
		 * 
		 * // Create direct kafka stream with brokers and topics
		 * JavaInputDStream<ConsumerRecord<String, String>> messages =
		 * KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(),
		 * ConsumerStrategies.Subscribe(topicsSet, kafkaParams));
		 * 
		 * // JavaDStream<String> lines = messages.map(ConsumerRecord::value); //
		 * System.out.println("Lines "+lines); // lines.foreachRDD(foreachFunc); //
		 * lines.print();
		 * 
		 * messages.foreachRDD(rdd -> { // System.out.println("--- New RDD with " +
		 * rdd.partitions().size() + // "partitions and " + rdd.count() // + " records "
		 * + jsc);
		 * 
		 * rdd.foreach(record -> {
		 * 
		 * String categoryId = record.value();
		 * 
		 * System.out.println("This should be json " + categoryId); JSONObject jsonObj =
		 * new JSONObject(categoryId); String category = (String)
		 * jsonObj.get("category"); String replyTopic = (String)
		 * jsonObj.get("reply-topic"); System.out.println("Thread - " +
		 * Thread.currentThread().getId());
		 * 
		 * Thread t = new Thread(() -> {
		 * ProductCategoryClustering.getCategoryPriceRange(category); });
		 * 
		 * t.start();
		 * 
		 * // categoryMap.put(record.key() == null ? "categoryId" : record.key(), //
		 * record.value()); // System.out.println("category " + category + "replyTopic "
		 * + replyTopic); // SparkContext ctx = SparkContext.getOrCreate(); //
		 * SQLContext sqlCtx = SQLContext.getOrCreate(ctx); //
		 * System.out.println("Get All " + ctx.getConf().getAll()); //
		 * System.out.println("ctx " + ctx); //
		 * System.out.println("Going to get price range for category " + categoryId);
		 * ProductCategoryClustering.getCategoryPriceRange(category); });
		 * 
		 * });
		 * 
		 * jssc.start(); jssc.awaitTermination();
		 */
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
