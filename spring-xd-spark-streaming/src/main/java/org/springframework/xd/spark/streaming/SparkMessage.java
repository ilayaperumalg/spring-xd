package org.springframework.xd.spark.streaming;

import java.io.Serializable;

import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;

/**
 * @author Ilayaperumal Gopinathan
 */
public class SparkMessage implements Message, Serializable {

	private Message  message;

	public SparkMessage(Message message) {
		this.message = message;
	}

	@Override
	public Object getPayload() {
		return message.getPayload();
	}

	@Override
	public MessageHeaders getHeaders() {
		return message.getHeaders();
	}

}
