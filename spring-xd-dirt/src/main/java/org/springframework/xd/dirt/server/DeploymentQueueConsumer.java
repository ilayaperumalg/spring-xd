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

import java.util.Collections;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.queue.QueueConsumer;
import org.apache.curator.framework.state.ConnectionState;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.xd.dirt.core.ResourceDeployer;
import org.springframework.xd.dirt.stream.JobDefinition;
import org.springframework.xd.dirt.stream.JobDeployer;
import org.springframework.xd.dirt.stream.StreamDefinition;
import org.springframework.xd.dirt.stream.StreamDeployer;

/**
 * @author Ilayaperumal Gopinathan
 */
public class DeploymentQueueConsumer implements QueueConsumer<DeploymentMessage> {

	private static final Log logger = LogFactory.getLog(DeploymentQueueConsumer.class);

	@Autowired
	private StreamDeployer streamDeployer;

	@Autowired
	private JobDeployer jobDeployer;

	@SuppressWarnings("rawtypes")
	public synchronized void consumeMessage(DeploymentMessage message, ResourceDeployer resourceDeployer) throws Exception {
		streamDeployer = (resourceDeployer instanceof StreamDeployer) ? (StreamDeployer) resourceDeployer : null;
		jobDeployer = (resourceDeployer instanceof JobDeployer) ? (JobDeployer) resourceDeployer : null;
		this.consumeMessage(message);
	}

	@Override
	public synchronized void consumeMessage(DeploymentMessage message) throws Exception {
		DeploymentAction deploymentAction = message.getDeploymentAction();
		String name = message.getUnitName();
		switch (message.getDeploymentUnitType()) {
			case Stream:
				switch (deploymentAction) {
					case create:
					case createAndDeploy: {
						streamDeployer.save(new StreamDefinition(name, message.getDefinition()));
						if (DeploymentAction.createAndDeploy.equals(deploymentAction)) {
							streamDeployer.deploy(name, Collections.<String, String>emptyMap());
						}
						break;
					}
					case deploy:
						streamDeployer.deploy(name, message.getDeploymentProperties());
						break;
					case undeploy:
						streamDeployer.undeploy(name);
						break;
					case undeployAll:
						streamDeployer.undeployAll();
						break;
					case destroy:
						streamDeployer.delete(name);
						break;
					case destroyAll:
						streamDeployer.deleteAll();
						break;
				}
				break;
			case Job:
				switch (deploymentAction) {
					case create:
					case createAndDeploy: {
						jobDeployer.save(new JobDefinition(name,  message.getDefinition()));
						if (DeploymentAction.createAndDeploy.equals(deploymentAction)) {
							jobDeployer.deploy(name, Collections.<String, String>emptyMap());
						}
						break;
					}
					case deploy:
						jobDeployer.deploy(name, message.getDeploymentProperties());
						break;
					case undeploy:
						jobDeployer.undeploy(name);
						break;
					case undeployAll:
						jobDeployer.undeployAll();
						break;
					case destroy:
						jobDeployer.delete(name);
						break;
					case destroyAll:
						jobDeployer.deleteAll();
						break;
				}
				break;
		}
	}

	@Override
	public void stateChanged(CuratorFramework client, ConnectionState newState) {
		logger.warn("Deployment Queue consumer state changed: " + newState);
	}
}
