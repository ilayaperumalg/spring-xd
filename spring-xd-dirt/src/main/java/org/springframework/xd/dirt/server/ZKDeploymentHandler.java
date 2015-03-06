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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
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

import org.springframework.util.Assert;
import org.springframework.xd.dirt.cluster.Container;
import org.springframework.xd.dirt.cluster.NoContainerException;
import org.springframework.xd.dirt.core.DeploymentUnitStatus;
import org.springframework.xd.dirt.core.Job;
import org.springframework.xd.dirt.core.JobDeploymentsPath;
import org.springframework.xd.dirt.core.ModuleDeploymentRequestsPath;
import org.springframework.xd.dirt.core.Stream;
import org.springframework.xd.dirt.core.StreamDeploymentsPath;
import org.springframework.xd.dirt.zookeeper.ChildPathIterator;
import org.springframework.xd.dirt.zookeeper.Paths;
import org.springframework.xd.dirt.zookeeper.ZooKeeperUtils;
import org.springframework.xd.module.ModuleDeploymentProperties;
import org.springframework.xd.module.ModuleDescriptor;
import org.springframework.xd.module.ModuleType;
import org.springframework.xd.module.RuntimeModuleDeploymentProperties;

/**
 * @author Patrick Peralta
 * @author Mark Fisher
 * @author Ilayaperumal Gopinathan
 */
public class ZKDeploymentHandler implements DeploymentHandler {

	/**
	 * Logger.
	 */
	private static final Logger logger = LoggerFactory.getLogger(ZKDeploymentHandler.class);

	private final ZKDeploymentUtil zkDeploymentUtil;

	private final PathChildrenCache moduleDeploymentRequestsPath;

	public ZKDeploymentHandler(ZKDeploymentUtil zkDeploymentUtil, PathChildrenCache moduleDeploymentRequestsPath) {
		this.zkDeploymentUtil = zkDeploymentUtil;
		this.moduleDeploymentRequestsPath = moduleDeploymentRequestsPath;
	}

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

	public void undeploy(DeploymentUnitType deploymentUnitType, String deploymentUnitName) throws Exception {
		ModuleDeploymentRequestsPath path;
		for (ChildData requestedModulesData : moduleDeploymentRequestsPath.getCurrentData()) {
			path = new ModuleDeploymentRequestsPath(requestedModulesData.getPath());
			if (path.getDeploymentUnitName().equals(deploymentUnitName)) {
				zkDeploymentUtil.getZkConnection().getClient().delete().deletingChildrenIfNeeded().forPath(path.build());
			}
		}
	}


	public void deploy(DeploymentUnitType deploymentUnitType, String deploymentUnitName) throws Exception {
		CuratorFramework client = zkDeploymentUtil.getZkConnection().getClient();
		switch (deploymentUnitType) {
			case Stream:
				deployStream(client, DeploymentLoader.loadStream(client, deploymentUnitName, zkDeploymentUtil.getStreamFactory()));
				break;
			case Job:
				deployJob(client, DeploymentLoader.loadJob(client, deploymentUnitName, zkDeploymentUtil.getJobFactory()));
				break;
		}

	}

	/**
	 * Issue deployment requests for a job. This deployment will occur if:
	 * <ul>
	 *     <li>the job has not been destroyed</li>
	 *     <li>the job has not been undeployed</li>
	 *     <li>there is a container that can deploy the job</li>
	 * </ul>
	 *
	 * @param job the job instance to redeploy
	 * @throws InterruptedException
	 */
	private void deployJob(CuratorFramework client, final Job job) throws InterruptedException {
		if (job != null) {
			// Ensure that the path for modules used by the container to write
			// ephemeral nodes exists. The presence of this path is assumed
			// by the supervisor when it calculates stream state when it is
			// assigned leadership. See XD-2170 for details.
			try {
				client.create().creatingParentsIfNeeded().forPath(Paths.build(
						Paths.JOB_DEPLOYMENTS, job.getName(), Paths.MODULES));
			}
			catch (Exception e) {
				ZooKeeperUtils.wrapAndThrowIgnoring(e, KeeperException.NodeExistsException.class);
			}

			String statusPath = Paths.build(Paths.JOB_DEPLOYMENTS, job.getName(), Paths.STATUS);

			DeploymentUnitStatus deployingStatus = null;
			try {
				deployingStatus = new DeploymentUnitStatus(ZooKeeperUtils.bytesToMap(
						client.getData().forPath(statusPath)));
			}
			catch (Exception e) {
				// an exception indicates that the status has not been set
			}
			Assert.state(deployingStatus != null
							&& deployingStatus.getState() == DeploymentUnitStatus.State.deploying,
					String.format("Expected 'deploying' status for job '%s'; current status: %s",
							job.getName(), deployingStatus));

			ModuleDeploymentPropertiesProvider<ModuleDeploymentProperties> provider =
					new DefaultModuleDeploymentPropertiesProvider(job);

			try {
				Collection<ModuleDeploymentStatus> deploymentStatuses = new ArrayList<ModuleDeploymentStatus>();
				for (ModuleDescriptor descriptor : job.getModuleDescriptors()) {
					RuntimeModuleDeploymentProperties deploymentProperties = new RuntimeModuleDeploymentProperties();
					deploymentProperties.putAll(provider.propertiesForDescriptor(descriptor));
					Deque<Container> matchedContainers = new ArrayDeque<Container>(zkDeploymentUtil.getContainerMatcher().match(descriptor,
							deploymentProperties,
							zkDeploymentUtil.getContainerRepository().findAll()));
					// Modules count == 0
					if (deploymentProperties.getCount() == 0) {
						deploymentProperties.setSequence(0);
						createModuleDeploymentRequestsPath(client, descriptor, deploymentProperties);
					}
					// Modules count > 0
					else {
						for (int i = 1; i <= deploymentProperties.getCount(); i++) {
							deploymentProperties.setSequence(i);
							createModuleDeploymentRequestsPath(client, descriptor, deploymentProperties);
						}
					}
					RuntimeModuleDeploymentPropertiesProvider deploymentRuntimeProvider =
							new RuntimeModuleDeploymentPropertiesProvider(provider);

					try {
						deploymentStatuses.addAll(zkDeploymentUtil.getModuleDeploymentWriter().writeDeployment(
								descriptor, deploymentRuntimeProvider, matchedContainers));
					}
					catch (NoContainerException e) {
						logger.warn("No containers available for deployment of job {}", job.getName());
					}

					DeploymentUnitStatus status = zkDeploymentUtil.getStateCalculator().calculate(job, provider, deploymentStatuses);

					logger.info("Deployment status for job '{}': {}", job.getName(), status);

					client.setData().forPath(statusPath, ZooKeeperUtils.mapToBytes(status.toMap()));
				}
			}
			catch (InterruptedException e) {
				throw e;
			}
			catch (Exception e) {
				throw ZooKeeperUtils.wrapThrowable(e);
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
			Job job = DeploymentLoader.loadJob(client, jobName, zkDeploymentUtil.getJobFactory());
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
				DeploymentUnitStatus status = zkDeploymentUtil.getStateCalculator().calculate(job,
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

	/**
	 * Issue deployment requests for the modules of the given stream.
	 *
	 * @param stream stream to be deployed
	 *
	 * @throws InterruptedException
	 */
	private void deployStream(CuratorFramework client, Stream stream) throws InterruptedException {
		// Ensure that the path for modules used by the container to write
		// ephemeral nodes exists. The presence of this path is assumed
		// by the supervisor when it calculates stream state when it is
		// assigned leadership. See XD-2170 for details.
		try {
			client.create().creatingParentsIfNeeded().forPath(
					Paths.build(Paths.STREAM_DEPLOYMENTS, stream.getName(), Paths.MODULES));
		}
		catch (Exception e) {
			ZooKeeperUtils.wrapAndThrowIgnoring(e, KeeperException.NodeExistsException.class);
		}

		String statusPath = Paths.build(Paths.STREAM_DEPLOYMENTS, stream.getName(), Paths.STATUS);

		// assert that the deployment status has been correctly set to "deploying"
		DeploymentUnitStatus deployingStatus = null;
		try {
			deployingStatus = new DeploymentUnitStatus(ZooKeeperUtils.bytesToMap(
					client.getData().forPath(statusPath)));
		}
		catch (Exception e) {
			// an exception indicates that the status has not been set
		}
		Assert.state(deployingStatus != null
						&& deployingStatus.getState() == DeploymentUnitStatus.State.deploying,
				String.format("Expected 'deploying' status for stream '%s'; current status: %s",
						stream.getName(), deployingStatus));

		try {
			Collection<ModuleDeploymentStatus> deploymentStatuses = new ArrayList<ModuleDeploymentStatus>();
			DefaultModuleDeploymentPropertiesProvider deploymentPropertiesProvider =
					new DefaultModuleDeploymentPropertiesProvider(stream);
			for (Iterator<ModuleDescriptor> descriptors = stream.getDeploymentOrderIterator(); descriptors.hasNext(); ) {
				ModuleDescriptor descriptor = descriptors.next();
				ModuleDeploymentProperties deploymentProperties = deploymentPropertiesProvider.propertiesForDescriptor(descriptor);

				// write out all of the required modules for this stream (including runtime properties);
				// this does not actually perform a deployment...this data is used in case there are not
				// enough containers to deploy the stream
				StreamRuntimePropertiesProvider partitionPropertiesProvider =
						new StreamRuntimePropertiesProvider(stream, deploymentPropertiesProvider);
				int moduleCount = deploymentProperties.getCount();
				if (moduleCount == 0) {
					createModuleDeploymentRequestsPath(client, descriptor,
							partitionPropertiesProvider.propertiesForDescriptor(descriptor));
				}
				else {
					for (int i = 0; i < moduleCount; i++) {
						createModuleDeploymentRequestsPath(client, descriptor,
								partitionPropertiesProvider.propertiesForDescriptor(descriptor));
					}
				}

				try {
					// find the containers that can deploy these modules
					Collection<Container> containers = zkDeploymentUtil.getContainerMatcher().match(descriptor, deploymentProperties,
							zkDeploymentUtil.getContainerRepository().findAll());

					// write out the deployment requests targeted to the containers obtained above;
					// a new instance of StreamPartitionPropertiesProvider is created since this
					// object is responsible for generating unique sequence ids for modules
					StreamRuntimePropertiesProvider deploymentRuntimeProvider =
							new StreamRuntimePropertiesProvider(stream, deploymentPropertiesProvider);

					deploymentStatuses.addAll(zkDeploymentUtil.getModuleDeploymentWriter().writeDeployment(
							descriptor, deploymentRuntimeProvider, containers));
				}
				catch (NoContainerException e) {
					logger.warn("No containers available for deployment of module '{}' for stream '{}'",
							descriptor.getModuleLabel(), stream.getName());
				}
			}

			DeploymentUnitStatus status = zkDeploymentUtil.getStateCalculator().calculate(stream, deploymentPropertiesProvider,
					deploymentStatuses);
			logger.info("Deployment status for stream '{}': {}", stream.getName(), status);

			client.setData().forPath(statusPath, ZooKeeperUtils.mapToBytes(status.toMap()));
		}
		catch (InterruptedException e) {
			throw e;
		}
		catch (Exception e) {
			throw ZooKeeperUtils.wrapThrowable(e);
		}
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
			Stream stream = DeploymentLoader.loadStream(client, streamName, zkDeploymentUtil.getStreamFactory());
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

				DeploymentUnitStatus status = zkDeploymentUtil.getStateCalculator().calculate(stream,
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

}
