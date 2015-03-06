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

import org.springframework.xd.dirt.container.store.ContainerRepository;
import org.springframework.xd.dirt.job.JobFactory;
import org.springframework.xd.dirt.stream.StreamFactory;
import org.springframework.xd.dirt.zookeeper.ZooKeeperConnection;

/**
 * @author Ilayaperumal Gopinathan
 */
public class ZKDeploymentUtil {

	private final ZooKeeperConnection zkConnection;

	private final ContainerRepository containerRepository;

	private final ModuleDeploymentWriter moduleDeploymentWriter;

	private final StreamFactory streamFactory;

	private final JobFactory jobFactory;

	private final ContainerMatcher containerMatcher;

	private final DeploymentUnitStateCalculator stateCalculator;


	public ZKDeploymentUtil(ZooKeeperConnection zkConnection, ContainerRepository containerRepository,
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

//	public PathChildrenCache getModuleDeploymentRequests() {
//		return moduleDeploymentRequests;
//	}
//
//	public PathChildrenCache getStreamDeployments() {
//		return streamDeployments;
//	}
//
//	public PathChildrenCache getJobDeployments() {
//		return jobDeployments;
//	}
//
//	public PathChildrenCache getContainers() {
//		return containers;
//	}

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
