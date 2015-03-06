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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.util.Assert;
import org.springframework.xd.dirt.cluster.Container;
import org.springframework.xd.dirt.cluster.NoContainerException;
import org.springframework.xd.dirt.core.DeploymentUnitStatus;
import org.springframework.xd.dirt.core.Job;
import org.springframework.xd.dirt.zookeeper.Paths;
import org.springframework.xd.dirt.zookeeper.ZooKeeperUtils;
import org.springframework.xd.module.ModuleDeploymentProperties;
import org.springframework.xd.module.ModuleDescriptor;
import org.springframework.xd.module.RuntimeModuleDeploymentProperties;

/**
 * Deployment handler that is responsible for the deployment of a Job.
 *
 * @author Patrick Peralta
 * @author Mark Fisher
 * @author Ilayaperumal Gopinathan
 */
public class JobDeploymentHandler extends ZKDeploymentHandler {

	/**
	 * Logger.
	 */
	private static final Logger logger = LoggerFactory.getLogger(JobDeploymentHandler.class);

	/**
	 * Deploy the Job with the given name.
	 *
	 * @param jobName the job name
	 * @throws Exception
	 */
	@Override
	public void deploy(String jobName) throws Exception {
		CuratorFramework client = zkDeploymentUtility.getZkConnection().getClient();
		deployJob(client, DeploymentLoader.loadJob(client, jobName, zkDeploymentUtility.getJobFactory()));
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
					Deque<Container> matchedContainers = new ArrayDeque<Container>(zkDeploymentUtility.getContainerMatcher().match(descriptor,
							deploymentProperties,
							zkDeploymentUtility.getContainerRepository().findAll()));
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
						deploymentStatuses.addAll(zkDeploymentUtility.getModuleDeploymentWriter().writeDeployment(
								descriptor, deploymentRuntimeProvider, matchedContainers));
					}
					catch (NoContainerException e) {
						logger.warn("No containers available for deployment of job {}", job.getName());
					}

					DeploymentUnitStatus status = zkDeploymentUtility.getStateCalculator().calculate(job, provider, deploymentStatuses);

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
}
