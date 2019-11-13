package co.hotwax.kafka;

import co.hotwax.message.GenericMessage;

public class KafkaMessage extends GenericMessage {

	private Object key;
	private Object value;
	private String toTopic;
	private String replyTopic;

	public String getReplyTopic() {
		return replyTopic;
	}

	public void setReplyTopic(String replyTopic) {
		this.replyTopic = replyTopic;
	}

	public String getToTopic() {
		return toTopic;
	}

	public void setToTopic(String toTopic) {
		this.toTopic = toTopic;
	}

	public Object getKey() {
		return key;
	}

	public void setKey(Object key) {
		this.key = key;
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

}
