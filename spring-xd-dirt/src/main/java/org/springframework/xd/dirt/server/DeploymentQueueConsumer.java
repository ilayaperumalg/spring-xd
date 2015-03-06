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

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.queue.QueueConsumer;
import org.apache.curator.framework.state.ConnectionState;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.xd.dirt.job.BatchJobAlreadyExistsException;
import org.springframework.xd.dirt.plugins.job.DistributedJobLocator;
import org.springframework.xd.dirt.stream.JobDefinition;
import org.springframework.xd.dirt.stream.JobDeployer;
import org.springframework.xd.dirt.stream.StreamDefinition;
import org.springframework.xd.dirt.stream.StreamDeployer;

/**
 * @author Ilayaperumal Gopinathan
 */
public class DeploymentQueueConsumer implements QueueConsumer<DeploymentMessage> {

	private final StreamDeployer streamDeployer;

	private final JobDeployer jobDeployer;

	@Autowired
	private DistributedJobLocator distributedJobLocator;

	public DeploymentQueueConsumer(StreamDeployer streamDeployer, JobDeployer jobDeployer) {
		this.streamDeployer = streamDeployer;
		this.jobDeployer = jobDeployer;
	}

	@Override
	public synchronized void consumeMessage(DeploymentMessage message) throws Exception {
		String deploymentUnit = message.getDeploymentUnit();
		String action = message.getAction();
		String name = message.getUnitName();
		if (deploymentUnit.equalsIgnoreCase("stream")) {
			if (action.equalsIgnoreCase("create") || action.equalsIgnoreCase("createAndDeploy")) {
				String definition = message.getDefinition();
				final StreamDefinition streamDefinition = streamDeployer.save(new StreamDefinition(name, definition));
				if (action.equalsIgnoreCase("createAndDeploy")) {
					streamDeployer.deploy(name, Collections.<String, String>emptyMap());
				}
			}
			else if (action.equalsIgnoreCase("deploy")) {
				streamDeployer.deploy(name, message.getDeploymentProperties());
			}
			else if (action.equalsIgnoreCase("destroy")) {
				streamDeployer.delete(name);
			}
			else if (action.equalsIgnoreCase("destroyAll")) {
				streamDeployer.deleteAll();
			}
			else if (action.equalsIgnoreCase("undeploy")) {
				streamDeployer.undeploy(name);
			}
			else if (action.equalsIgnoreCase("undeployAll")) {
				streamDeployer.undeployAll();
			}
		}
		else {
			if (action.equalsIgnoreCase("create") || action.equalsIgnoreCase("createAndDeploy")) {
				// Verify if the batch job repository already has the job with the same name.
				if (distributedJobLocator.getJobNames().contains(name)) {
					throw new BatchJobAlreadyExistsException(name);
				}
				String definition = message.getDefinition();
				final JobDefinition jobDefinition = jobDeployer.save(new JobDefinition(name, definition));
				if (action.equalsIgnoreCase("createAndDeploy")) {
					jobDeployer.deploy(name, Collections.<String, String>emptyMap());
				}
			}
			else if (action.equalsIgnoreCase("deploy")) {
				jobDeployer.deploy(name, message.getDeploymentProperties());
			}
			else if (action.equalsIgnoreCase("destroy")) {
				jobDeployer.delete(name);
			}
			else if (action.equalsIgnoreCase("destroyAll")) {
				jobDeployer.deleteAll();
			}
			else if (action.equalsIgnoreCase("undeploy")) {
				jobDeployer.undeploy(name);
			}
			else if (action.equalsIgnoreCase("undeployAll")) {
				jobDeployer.undeployAll();
			}
		}
		System.out.println("********* message ********** " +
				message.getDeploymentUnit() + " " + message.getAction() + " " + message.getDefinition() + " " +
				message.getDeploymentProperties());
	}

	@Override
	public void stateChanged(CuratorFramework client, ConnectionState newState) {
		System.out.println("************* state changed ******* " + newState);
	}
}
