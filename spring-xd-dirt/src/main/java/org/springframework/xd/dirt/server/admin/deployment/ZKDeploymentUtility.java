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

import org.springframework.xd.dirt.container.store.ContainerRepository;
import org.springframework.xd.dirt.job.JobFactory;
import org.springframework.xd.dirt.stream.StreamFactory;
import org.springframework.xd.dirt.zookeeper.ZooKeeperConnection;

/**
 * Utility class for ZK based deployments.
 *
 * @author Ilayaperumal Gopinathan
 */
public class ZKDeploymentUtility {

	private final ZooKeeperConnection zkConnection;

	private final ContainerRepository containerRepository;

	private final ModuleDeploymentWriter moduleDeploymentWriter;

	private final StreamFactory streamFactory;

	private final JobFactory jobFactory;

	private final ContainerMatcher containerMatcher;

	private final DeploymentUnitStateCalculator stateCalculator;


	/**
	 * Construct ZKDeploymentUtility
	 *
	 * @param zkConnection           the ZooKeeper connection
	 * @param containerRepository    the container repository that holds the available containers details
	 * @param moduleDeploymentWriter the module deployment writer that writes the module deployment requests to ZK
	 * @param streamFactory          factory class to build stream instances
	 * @param jobFactory             factory class to build job instances
	 * @param containerMatcher       container matcher to match the criteria
	 * @param stateCalculator        deployment state calculator
	 */
	public ZKDeploymentUtility(ZooKeeperConnection zkConnection, ContainerRepository containerRepository,
			ModuleDeploymentWriter moduleDeploymentWriter,
			StreamFactory streamFactory, JobFactory jobFactory, ContainerMatcher containerMatcher,
			DeploymentUnitStateCalculator stateCalculator) {
		this.zkConnection = zkConnection;
		this.containerRepository = containerRepository;
		this.moduleDeploymentWriter = moduleDeploymentWriter;
		this.streamFactory = streamFactory;
		this.jobFactory = jobFactory;
		this.containerMatcher = containerMatcher;
		this.stateCalculator = stateCalculator;
	}

	public StreamFactory getStreamFactory() {
		return streamFactory;
	}

	public ModuleDeploymentWriter getModuleDeploymentWriter() {
		return moduleDeploymentWriter;
	}

	public JobFactory getJobFactory() {
		return jobFactory;
	}

	public ContainerMatcher getContainerMatcher() {
		return containerMatcher;
	}

	public DeploymentUnitStateCalculator getStateCalculator() {
		return stateCalculator;
	}

	public ZooKeeperConnection getZkConnection() {
		return zkConnection;
	}

	public ContainerRepository getContainerRepository() {
		return containerRepository;
	}

}
