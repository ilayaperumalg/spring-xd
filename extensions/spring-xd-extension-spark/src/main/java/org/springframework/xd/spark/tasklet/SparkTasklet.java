/*
 * Copyright 2013-2014 the original author or authors.
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

package org.springframework.xd.spark.tasklet;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Environment;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * {@link Tasklet} for running Spark job.
 *
 * @author Thomas Risberg
 * @author Ilayaperumal Gopinathan
 */
public class SparkTasklet implements Tasklet, EnvironmentAware, StepExecutionListener {

	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	private static final String MODULE_HOME = "xd.module.home";

	private static final String LIB_PATTERN = "/job/sparkapp/lib/*.jar";

	/**
	 * Exit code of Spark app
	 */
	private int exitCode = -1;

	/**
	 * Spark master URL
	 */
	private String master;

	/**
	 * Spark application's main class
	 */
	private String mainClass;

	/**
	 * Path to a bundled jar that includes your application and its
	 * dependencies excluding spark
	 */
	private String appJar;

	/**
	 * Program arguments for the application main class.
	 */
	private String programArgs;

	private ConfigurableEnvironment environment;

	private ResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();

	public String getMaster() {
		return master;
	}

	public void setMaster(String master) {
		this.master = master;
	}

	public String getMainClass() {
		return mainClass;
	}

	public void setMainClass(String mainClass) {
		this.mainClass = mainClass;
	}

	public String getAppJar() {
		return appJar;
	}

	public void setAppJar(String appJar) {
		this.appJar = appJar;
	}

	public String getProgramArgs() {
		return programArgs;
	}

	public void setProgramArgs(String programArgs) {
		this.programArgs = programArgs;
	}

	@Override
	public void setEnvironment(Environment environment) {
		this.environment = (ConfigurableEnvironment) environment;
	}

	/**
	 * Before executing the step, make sure the spark application
	 * main class is loaded from the application jar provided.
	 */
	@Override
	public void beforeStep(StepExecution stepExecution) {
		ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		if (appJar != null) {
			URL[] url;
			try {
				url = new URL[] { resolver.getResource(appJar).getURL() };
			}
			catch (IOException ioe) {
				throw new SparkAppJarAccessException(ioe.getMessage());
			}
			classLoader = new URLClassLoader(url, Thread.currentThread().getContextClassLoader());
		}
		try {
			Class.forName(mainClass, false, classLoader);
		}
		catch (ClassNotFoundException e) {
			throw new SparkAppClassNotFoundException(e.getMessage());
		}
	}

	@Override
	public RepeatStatus execute(StepContribution contribution,
			ChunkContext chunkContext) throws Exception {

		StepExecution stepExecution = chunkContext.getStepContext().getStepExecution();
		ExitStatus exitStatus = stepExecution.getExitStatus();

		String moduleHome = environment.getProperty(MODULE_HOME);
		Assert.notNull(moduleHome, "Module home must not be null.");
		Resource[] resources = resolver.getResources(moduleHome + LIB_PATTERN);
		ArrayList<String> dependencies = new ArrayList<String>();
		for (int i = 0; i < resources.length; i++) {
			dependencies.add(resources[i].getURL().getFile());
		}
		ArrayList<String> args = new ArrayList<String>();
		args.add("--class");
		args.add(mainClass);
		args.add("--master");
		args.add(master);
		args.add("--deploy-mode");
		args.add("client");
		args.add("--jars");
		args.add(StringUtils.collectionToCommaDelimitedString(dependencies));
		if (StringUtils.hasText(appJar)) {
			args.add(appJar);
		}
		if (StringUtils.hasText(programArgs)) {
			args.add(programArgs);
		}

		List<String> sparkCommand = new ArrayList<String>();
		sparkCommand.add("java");
		sparkCommand.add("org.apache.spark.deploy.SparkSubmit");
		sparkCommand.addAll(args);

		URLClassLoader thisClassLoader;
		URLClassLoader contextClassLoader;
		try {
			thisClassLoader = (URLClassLoader) Thread.currentThread().getContextClassLoader();
			contextClassLoader = (URLClassLoader) this.getClass().getClassLoader();
		} catch (Exception e) {
			throw new IllegalStateException("Unable to determine classpath from ClassLoader.", e);
		}
		List<String> classPath = new ArrayList<String>();
		for (URL url : thisClassLoader.getURLs()) {
			classPath.add(url.getPath());
		}
		for (URL url : contextClassLoader.getURLs()) {
			if (!classPath.contains(url.getPath())) {
				classPath.add(url.getPath());
			}
		}
		StringBuilder classPathBuilder = new StringBuilder();
		String separator = System.getProperty("path.separator");
		for (String url : classPath) {
			if (classPathBuilder.length() > 0) {
				classPathBuilder.append(separator);
			}
			classPathBuilder.append(url);
		}

		ProcessBuilder pb = new ProcessBuilder(sparkCommand).redirectErrorStream(true);
		Map<String, String> env = pb.environment();
		env.put("CLASSPATH", classPathBuilder.toString());
		String msg = "Spark application '" + mainClass + "' is being launched";
		try {
			Process p = pb.start();
			p.waitFor();
			exitCode = p.exitValue();
			InputStream log = p.getInputStream();
			String logMsg = CharStreams.toString(new InputStreamReader(log, Charsets.UTF_8));
			p.destroy();
			if (exitCode != 0) {
				System.err.println(logMsg);
			}
			StringBuffer errors = new StringBuffer();
			Scanner exceptionScanner = new Scanner(logMsg);
			while (exceptionScanner.hasNextLine()) {
				String line = exceptionScanner.nextLine();
				if (line.contains("Exception")) {
					errors.append(line).append("\n");
					while(exceptionScanner.hasNextLine()) {
						String stackTraceLine = exceptionScanner.nextLine();
						if (stackTraceLine.startsWith("\t")) {
							errors.append(stackTraceLine).append("\n");
						}
						else {
							break;
						}
					}
					break;
				}
			}
			exceptionScanner.close();
			msg = "Spark application '" + mainClass + "' finished with exit code: " + exitCode +
					(errors.length() > 0 ? '\n' + errors.toString() : "");
		} catch (IOException e) {
			msg = "Starting Spark application '" + mainClass + "' caused: " + e;
			logger.error(msg);
		} catch (InterruptedException e) {
			msg = "Executing Spark application '" + mainClass + "' caused: " + e;
			logger.error(msg);
		}
		finally {
			stepExecution.setExitStatus(exitStatus.addExitDescription(msg));
		}

		logger.info(msg);
		return RepeatStatus.FINISHED;
	}

	@Override
	public ExitStatus afterStep(StepExecution stepExecution) {
		if (exitCode == 0) {
			return ExitStatus.COMPLETED;
		}
		else {
			return ExitStatus.FAILED;
		}
	}
}
