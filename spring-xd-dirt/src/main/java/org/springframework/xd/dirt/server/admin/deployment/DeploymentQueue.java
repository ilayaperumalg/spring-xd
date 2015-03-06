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
package org.springframework.xd.dirt.server.admin.deployment;

import java.io.IOException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.queue.DistributedQueue;
import org.apache.curator.framework.recipes.queue.QueueBuilder;
import org.apache.curator.framework.recipes.queue.QueueConsumer;
import org.apache.curator.framework.recipes.queue.QueueSerializer;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.xd.dirt.zookeeper.Paths;
import org.springframework.xd.dirt.zookeeper.ZooKeeperConnection;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;

/**
 * This class controls the lifecycle operations of the ZK distributed queue that
 * holds the {@link org.springframework.xd.dirt.server.admin.deployment.DeploymentMessage}s.
 *
 * @author Ilayaperumal Gopinathan
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class DeploymentQueue implements InitializingBean, DisposableBean {

	private DistributedQueue<DeploymentMessage> distributedQueue;

	private final CuratorFramework client;

	private final QueueConsumer<DeploymentMessage> queueConsumer;

	private final String deploymentQueuePath;

	private final ObjectWriter objectWriter = new ObjectMapper().writerWithType(DeploymentMessage.class);

	private final ObjectReader objectReader = new ObjectMapper().reader(DeploymentMessage.class);

	/**
	 * Construct deployment queue
	 * @param zkConnection the ZooKeeper connection
	 */
	public DeploymentQueue(ZooKeeperConnection zkConnection) {
		this(zkConnection.getClient(), null, Paths.DEPLOYMENT_QUEUE);
	}

	/**
	 * Construct deployment queue
	 * @param client the Curator framework client
	 * @param queueConsumer the consumer that consumes the deployment messages
	 * @param deploymentQueuePath the ZK path for the deployment queue
	 */
	public DeploymentQueue(CuratorFramework client, QueueConsumer queueConsumer, String deploymentQueuePath) {
		this.client = client;
		this.queueConsumer = queueConsumer;
		this.deploymentQueuePath = deploymentQueuePath;
	}

	/**
	 * Build and Start the ZK distributed queue.
	 * @throws Exception
	 */
	public void start() throws Exception {
		if (client != null) {
			QueueBuilder<DeploymentMessage> builder = QueueBuilder.builder(client, queueConsumer,
					new DeploymentMessageSerializer(), deploymentQueuePath);
			this.distributedQueue = builder.buildQueue();
			this.distributedQueue.start();
		}
	}

	/**
	 * Get the underlying distributed queue.
	 * @return the distributed queue
	 */
	public DistributedQueue<DeploymentMessage> getDistributedQueue() {
		return distributedQueue;
	}

	@Override
	public void destroy() throws Exception {
		this.distributedQueue.close();
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		this.start();
	}


	/**
	 * The queue serializer implementation to serialize/de-serialize
	 * {@link org.springframework.xd.dirt.server.admin.deployment.DeploymentMessage}
	 */
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
				throw new RuntimeException(ioe);
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
				throw new RuntimeException(ioe);
			}
			return byteArray;
		}
	}
}
