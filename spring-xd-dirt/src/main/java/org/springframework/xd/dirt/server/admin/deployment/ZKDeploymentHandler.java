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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;
import org.springframework.xd.dirt.container.store.ContainerRepository;
import org.springframework.xd.dirt.core.DeploymentUnitStatus;
import org.springframework.xd.dirt.core.Job;
import org.springframework.xd.dirt.core.JobDeploymentsPath;
import org.springframework.xd.dirt.core.ModuleDeploymentRequestsPath;
import org.springframework.xd.dirt.core.Stream;
import org.springframework.xd.dirt.core.StreamDeploymentsPath;
import org.springframework.xd.dirt.job.JobFactory;
import org.springframework.xd.dirt.stream.StreamFactory;
import org.springframework.xd.dirt.zookeeper.ChildPathIterator;
import org.springframework.xd.dirt.zookeeper.Paths;
import org.springframework.xd.dirt.zookeeper.ZooKeeperConnection;
import org.springframework.xd.dirt.zookeeper.ZooKeeperUtils;
import org.springframework.xd.module.ModuleDescriptor;
import org.springframework.xd.module.ModuleType;
import org.springframework.xd.module.RuntimeModuleDeploymentProperties;

/**
 * Abstract class that has common implementations of ZK based deployments.
 *
 * @author Patrick Peralta
 * @author Mark Fisher
 * @author Ilayaperumal Gopinathan
 */
public abstract class ZKDeploymentHandler implements DeploymentHandler {

	/**
	 * Logger.
	 */
	private static final Logger logger = LoggerFactory.getLogger(ZKDeploymentHandler.class);

	/**
	 * ZooKeeper connection
	 */
	@Autowired
	protected ZooKeeperConnection zkConnection;

	/**
	 * Factory to construct {@link org.springframework.xd.dirt.core.Stream} instance
	 */
	@Autowired
	protected StreamFactory streamFactory;

	/**
	 * Factory to construct {@link org.springframework.xd.dirt.core.Job} instance
	 */
	@Autowired
	protected JobFactory jobFactory;

	/**
	 * Matcher that applies container matching criteria
	 */
	@Autowired
	protected ContainerMatcher containerMatcher;

	/**
	 * Repository for the containers
	 */
	@Autowired
	protected ContainerRepository containerRepository;

	/**
	 * Utility that writes module deployment requests to ZK path
	 */
	@Autowired
	protected ModuleDeploymentWriter moduleDeploymentWriter;

	/**
	 * Deployment unit state calculator
	 */
	@Autowired
	protected DeploymentUnitStateCalculator stateCalculator;


	/**
	 * Cache of children under the module deployment requests path.
	 */
	protected PathChildrenCache moduleDeploymentRequests;


	/**
	 * Create {@link org.springframework.xd.dirt.core.ModuleDeploymentRequestsPath} for the given
	 * {@link org.springframework.xd.module.ModuleDescriptor} and
	 * the {@link org.springframework.xd.module.RuntimeModuleDeploymentProperties}.
	 *
	 * @param client the curator client
	 * @param descriptor the module descriptor
	 * @param deploymentProperties the runtime deployment properties
	 */
	protected void createModuleDeploymentRequestsPath(CuratorFramework client, ModuleDescriptor descriptor,
			RuntimeModuleDeploymentProperties deploymentProperties) {
		// Create and set the data for the requested modules path
		String requestedModulesPath = new ModuleDeploymentRequestsPath()
				.setDeploymentUnitName(descriptor.getGroup())
				.setModuleType(descriptor.getType().toString())
				.setModuleLabel(descriptor.getModuleLabel())
				.setModuleSequence(deploymentProperties.getSequenceAsString())
				.build();
		try {
			client.create().creatingParentsIfNeeded().forPath(requestedModulesPath,
					ZooKeeperUtils.mapToBytes(deploymentProperties));
		}
		catch (Exception e) {
			throw ZooKeeperUtils.wrapThrowable(e);
		}
	}

	public void undeploy(String deploymentUnitName) throws Exception {
		Assert.notNull(moduleDeploymentRequests, "Module deployment request path cache shouldn't be null.");
		ModuleDeploymentRequestsPath path;
		for (ChildData requestedModulesData : moduleDeploymentRequests.getCurrentData()) {
			path = new ModuleDeploymentRequestsPath(requestedModulesData.getPath());
			if (path.getDeploymentUnitName().equals(deploymentUnitName)) {
				zkConnection.getClient().delete().deletingChildrenIfNeeded().forPath(path.build());
			}
		}
	}

	/**
	 * Set the module deployment requests {@link org.apache.curator.framework.recipes.cache.PathChildrenCache}.
	 *
	 * @param moduleDeploymentRequests the path children cache for module deployment requests path
	 */
	public void setModuleDeploymentRequests(PathChildrenCache moduleDeploymentRequests) {
		this.moduleDeploymentRequests = moduleDeploymentRequests;
	}

	/**
	 * Iterate all deployed streams, recalculate the state of each, and create
	 * an ephemeral node indicating the stream state. This is typically invoked
	 * upon leader election.
	 *
	 * @param client             curator client
	 * @param streamDeployments  curator cache of stream deployments
	 * @throws Exception
	 */
	public void recalculateStreamStates(CuratorFramework client, PathChildrenCache streamDeployments) throws Exception {
		for (Iterator<String> iterator =
					 new ChildPathIterator<String>(ZooKeeperUtils.stripPathConverter, streamDeployments); iterator.hasNext(); ) {
			String streamName = iterator.next();
			String definitionPath = Paths.build(Paths.build(Paths.STREAM_DEPLOYMENTS, streamName));
			Stream stream = DeploymentLoader.loadStream(client, streamName, streamFactory);
			if (stream != null) {
				String streamModulesPath = Paths.build(definitionPath, Paths.MODULES);
				List<ModuleDeploymentStatus> statusList = new ArrayList<ModuleDeploymentStatus>();
				try {
					List<String> moduleDeployments = client.getChildren().forPath(streamModulesPath);
					for (String moduleDeployment : moduleDeployments) {
						StreamDeploymentsPath streamDeploymentsPath = new StreamDeploymentsPath(
								Paths.build(streamModulesPath, moduleDeployment));
						statusList.add(new ModuleDeploymentStatus(
								streamDeploymentsPath.getContainer(),
								streamDeploymentsPath.getModuleSequence(),
								new ModuleDescriptor.Key(streamName,
										ModuleType.valueOf(streamDeploymentsPath.getModuleType()),
										streamDeploymentsPath.getModuleLabel()),
								ModuleDeploymentStatus.State.deployed, null));
					}
				}
				catch (KeeperException.NoNodeException e) {
					// indicates there are no modules deployed for this stream;
					// ignore as this will result in an empty statusList
				}

				DeploymentUnitStatus status = stateCalculator.calculate(stream,
						new DefaultModuleDeploymentPropertiesProvider(stream), statusList);

				logger.info("Deployment status for stream '{}': {}", stream.getName(), status);

				String statusPath = Paths.build(Paths.STREAM_DEPLOYMENTS, stream.getName(), Paths.STATUS);
				Stat stat = client.checkExists().forPath(statusPath);
				if (stat != null) {
					logger.trace("Found old status path {}; stat: {}", statusPath, stat);
					client.delete().forPath(statusPath);
				}
				client.create().withMode(CreateMode.EPHEMERAL).forPath(statusPath,
						ZooKeeperUtils.mapToBytes(status.toMap()));
			}
		}
	}


	/**
	 * Iterate all deployed jobs, recalculate the deployment status of each, and
	 * create an ephemeral node indicating the job state. This is typically invoked
	 * upon leader election.
	 *
	 * @param client          curator client
	 * @param jobDeployments  curator cache of job deployments
	 * @throws Exception
	 */
	public void recalculateJobStates(CuratorFramework client, PathChildrenCache jobDeployments) throws Exception {
		for (Iterator<String> iterator = new ChildPathIterator<String>(ZooKeeperUtils.stripPathConverter,
				jobDeployments); iterator.hasNext(); ) {
			String jobName = iterator.next();
			Job job = DeploymentLoader.loadJob(client, jobName, jobFactory);
			if (job != null) {
				String jobModulesPath = Paths.build(Paths.JOB_DEPLOYMENTS, jobName, Paths.MODULES);
				List<ModuleDeploymentStatus> statusList = new ArrayList<ModuleDeploymentStatus>();
				List<String> moduleDeployments = client.getChildren().forPath(jobModulesPath);
				for (String moduleDeployment : moduleDeployments) {
					JobDeploymentsPath jobDeploymentsPath = new JobDeploymentsPath(
							Paths.build(jobModulesPath, moduleDeployment));
					statusList.add(new ModuleDeploymentStatus(
							jobDeploymentsPath.getContainer(),
							jobDeploymentsPath.getModuleSequence(),
							new ModuleDescriptor.Key(jobName, ModuleType.job, jobDeploymentsPath.getModuleLabel()),
							ModuleDeploymentStatus.State.deployed, null));
				}
				DeploymentUnitStatus status = stateCalculator.calculate(job,
						new DefaultModuleDeploymentPropertiesProvider(job), statusList);

				logger.info("Deployment status for job '{}': {}", job.getName(), status);

				String statusPath = Paths.build(Paths.JOB_DEPLOYMENTS, job.getName(), Paths.STATUS);
				Stat stat = client.checkExists().forPath(statusPath);
				if (stat != null) {
					logger.trace("Found old status path {}; stat: {}", statusPath, stat);
					client.delete().forPath(statusPath);
				}
				client.create().withMode(CreateMode.EPHEMERAL).forPath(statusPath,
						ZooKeeperUtils.mapToBytes(status.toMap()));
			}
		}
	}
}
