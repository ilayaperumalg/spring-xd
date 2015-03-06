/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.xd.dirt.server;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * @author Ilayaperumal Gopinathan
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY)
public class DeploymentMessage {

	private String deploymentUnit;

	private String unitName;

	private String definition;

	private String action;

	private Map<String, String> deploymentProperties;

	protected DeploymentMessage() {

	}

	public DeploymentMessage(String deploymentUnit, String unitName, String definition, String action, Map<String, String> deploymentProperties) {
		this.deploymentUnit = deploymentUnit;
		this.unitName = unitName;
		this.definition = definition;
		this.action = action;
		this.deploymentProperties = deploymentProperties;
	}

	public String getUnitName() {
		return unitName;
	}

	public String getDefinition() {
		return definition;
	}

	public String getAction() {
		return action;
	}

	public Map<String, String> getDeploymentProperties() {
		return deploymentProperties;
	}

	public String getDeploymentUnit() {

		return deploymentUnit;
	}
}
