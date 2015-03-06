/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.springframework.xd.dirt.server;

import java.io.IOException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.queue.DistributedQueue;
import org.apache.curator.framework.recipes.queue.QueueBuilder;
import org.apache.curator.framework.recipes.queue.QueueConsumer;
import org.apache.curator.framework.recipes.queue.QueueSerializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;

/**
 * @author Ilayaperumal Gopinathan
 */
@SuppressWarnings("unchecked")
public class DeploymentQueue {

	private final DistributedQueue<DeploymentMessage> distributedQueue;

	private final ObjectWriter objectWriter = new ObjectMapper().writerWithType(DeploymentMessage.class);

	private final ObjectReader objectReader = new ObjectMapper().reader(DeploymentMessage.class);

	public DeploymentQueue(CuratorFramework client, QueueConsumer queueConsumer, String deploymentQueuePath) {
		QueueBuilder<DeploymentMessage> builder = QueueBuilder.builder(client, queueConsumer,
				new DeploymentMessageSerializer(), deploymentQueuePath);
		this.distributedQueue = builder.buildQueue();
	}

	public void start() throws Exception {
		this.distributedQueue.start();
	}

	public DistributedQueue<DeploymentMessage> getDistributedQueue() {
		return distributedQueue;
	}

	private class DeploymentMessageSerializer implements QueueSerializer<DeploymentMessage> {

		public DeploymentMessage deserialize(byte[] buffer) {
			DeploymentMessage deploymentMessage = null;
			try {
				deploymentMessage = objectReader.readValue(buffer);
			}
			catch (JsonProcessingException e) {
				throw new RuntimeException(e);
			}
			catch (IOException ioe) {
				//todo:
			}
			return deploymentMessage;
		}

		public byte[] serialize(DeploymentMessage message) {
			byte[] byteArray = null;
			try {
				byteArray = objectWriter.writeValueAsBytes(message);
			}
			catch (JsonMappingException e) {
				throw new RuntimeException(e);
			}
			catch (IOException ioe) {
				//todo:
			}
			return byteArray;
		}
	}
}
