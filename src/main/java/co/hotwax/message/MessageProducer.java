package co.hotwax.message;

import co.hotwax.kafka.KafkaMessage;

/**
 * 
 * @author grv
 *
 */
public interface MessageProducer {

	boolean produce(KafkaMessage message);

}
