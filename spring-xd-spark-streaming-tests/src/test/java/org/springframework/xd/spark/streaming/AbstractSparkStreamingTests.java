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
package org.springframework.xd.spark.streaming;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.springframework.shell.Bootstrap;
import org.springframework.shell.core.JLineShellComponent;
import org.springframework.xd.dirt.module.ArchiveModuleRegistry;
import org.springframework.xd.dirt.server.SingleNodeApplication;
import org.springframework.xd.dirt.test.SingleNodeIntegrationTestSupport;
import org.springframework.xd.shell.command.StreamCommandTemplate;
import org.springframework.xd.shell.command.fixtures.HttpSource;
import org.springframework.xd.shell.command.fixtures.XDMatchers;
import org.springframework.xd.test.RandomConfigurationSupport;
import org.springframework.xd.test.fixtures.FileSink;


/**
 * Abstract Spark streaming test class which can be extended to run against multiple XD transport.
 *
 * @author Ilayaperumal Gopinathan
 */
public abstract class AbstractSparkStreamingTests {

	protected static final String TEST_MESSAGE = "foo foo foo";

	private SingleNodeApplication singleNodeApplication;

	private SingleNodeIntegrationTestSupport integrationTestSupport;

	private JLineShellComponent shell;

	protected StreamCommandTemplate streamOps;

	private final String transport;

	public AbstractSparkStreamingTests(String transport) {
		this.transport = transport;
	}

	@Rule
	public TestName testName = new TestName();

	@Before
	public void setup() throws Exception {
		RandomConfigurationSupport randomConfigSupport = new RandomConfigurationSupport();
		singleNodeApplication = new SingleNodeApplication().run("--transport", this.transport, "--analytics", "redis");
		integrationTestSupport = new SingleNodeIntegrationTestSupport(singleNodeApplication);
		integrationTestSupport.addModuleRegistry(new ArchiveModuleRegistry("classpath:/spring-xd/xd/modules"));
		Bootstrap bootstrap = new Bootstrap(new String[] {"--port", randomConfigSupport.getAdminServerPort()});
		shell = bootstrap.getJLineShellComponent();
		if (!shell.isRunning()) {
			shell.start();
		}
		streamOps = new StreamCommandTemplate(shell, integrationTestSupport);
	}

	@After
	public void tearDown() {
		singleNodeApplication.close();
		shell.stop();
	}

	protected void createStream(String streamName, String stream) {
		streamOps.create(streamName, stream);
	}

	@Test
	public void testSparkProcessor() throws Exception {
		HttpSource source = new HttpSource(shell);
		String streamName =  testName.getMethodName() + new Random().nextInt();
		FileSink sink = new FileSink().binary(true);
		try {
			String stream = String.format("%s | spark-word-count | %s --inputType=text/plain", source, sink);
			createStream(streamName, stream);
			source.ensureReady().postData(TEST_MESSAGE);
			assertThat(sink, XDMatchers.eventually(XDMatchers.hasContentsThat(equalTo("(foo,3)"))));
		}
		finally {
			streamOps.destroyStream(streamName);
			sink.cleanup();
		}
	}

//	@Test
//	public void testSparkProcessorWithInputType() throws Exception {
//		HttpSource source = new HttpSource(shell);
//		String streamName =  testName.getMethodName()  + new Random().nextInt();
//		FileSink sink = new FileSink().binary(true);
//		try {
//			String stream = String.format("%s | spark-word-count --inputType=text/plain | %s " +
//					"--inputType=text/plain", source, sink);
//			createStream(streamName, stream);
//			source.ensureReady().postData(TEST_MESSAGE);
//			assertThat(sink, XDMatchers.eventually(XDMatchers.hasContentsThat(equalTo("(foo,3)"))));
//		}
//		finally {
//			streamOps.destroyStream(streamName);
//			sink.cleanup();
//		}
//	}
//
//	@Test
//	public void testSparkProcessorWithOutputType() throws Exception {
//		HttpSource source = new HttpSource(shell);
//		String streamName =  testName.getMethodName()  + new Random().nextInt();
//		FileSink sink = new FileSink().binary(true);
//		try {
//			String stream = String.format("%s | spark-word-count --outputType=application/json | %s", source, sink);
//			createStream(streamName, stream);
//			source.ensureReady().postData(TEST_MESSAGE);
//			assertThat(sink, XDMatchers.eventually(XDMatchers.hasContentsThat(equalTo("{\"_1\":\"foo\",\"_2\":3}"))));
//		}
//		finally {
//			streamOps.destroyStream(streamName);
//			sink.cleanup();
//		}
//	}
//
//	@Test
//	public void testSparkScalaProcessor() throws Exception {
//		HttpSource source = new HttpSource(shell);
//		String streamName =  testName.getMethodName()  + new Random().nextInt();
//		FileSink sink = new FileSink().binary(true);
//		try {
//			String stream = String.format("%s | spark-scala-word-count | %s --inputType=text/plain", source, sink);
//			createStream(streamName, stream);
//			source.ensureReady().postData(TEST_MESSAGE);
//			assertThat(sink, XDMatchers.eventually(XDMatchers.hasContentsThat(equalTo("(foo,3)"))));
//		}
//		finally {
//			streamOps.destroyStream(streamName);
//			sink.cleanup();
//		}
//	}
//
//	@Test
//	public void testSparkScalaProcessorWithInputType() throws Exception {
//		HttpSource source = new HttpSource(shell);
//		String streamName =  testName.getMethodName()  + new Random().nextInt();
//		FileSink sink = new FileSink().binary(true);
//		try {
//			String stream = String.format("%s | spark-scala-word-count --inputType=text/plain | " +
//					"%s --inputType=text/plain", source, sink);
//			createStream(streamName, stream);
//			source.ensureReady().postData(TEST_MESSAGE);
//			assertThat(sink, XDMatchers.eventually(XDMatchers.hasContentsThat(equalTo("(foo,3)"))));
//		}
//		finally {
//			streamOps.destroyStream(streamName);
//			sink.cleanup();
//		}
//	}
//
//	@Test
//	public void testSparkScalaProcessorWithOutputType() throws Exception {
//		HttpSource source = new HttpSource(shell);
//		String streamName =  testName.getMethodName()  + new Random().nextInt();
//		FileSink sink = new FileSink().binary(true);
//		try {
//			String stream = String.format("%s | spark-scala-word-count --outputType=application/json | %s", source, sink);
//			createStream(streamName, stream);
//			source.ensureReady().postData(TEST_MESSAGE);
//			assertThat(sink, XDMatchers.eventually(XDMatchers.hasContentsThat(equalTo("{\"_1\":\"foo\",\"_2\":3}"))));
//		}
//		finally {
//			streamOps.destroyStream(streamName);
//			sink.cleanup();
//		}
//	}
//
//	@Test
//	public void testSparkFileLogger() throws Exception {
//		String streamName = testName.getMethodName()  + new Random().nextInt();
//		String fileName = streamName + ".txt";
//		File file = new File(fileName);
//		try {
//			final HttpSource source = new HttpSource(shell);
//			final String stream = String.format("%s | file-logger --path=%s", source, fileName);
//			createStream(streamName, stream);
//			source.ensureReady().postData(TEST_MESSAGE);
//			assertThat(file, eventually(50, 100, fileContent(endsWith(TEST_MESSAGE + System.lineSeparator()))));
//		}
//		finally {
//			streamOps.destroyStream(streamName);
//			if (file.exists()) {
//				file.delete();
//			}
//		}
//	}
//
//	@Test
//	public void testSparkFileLoggerWithInputType() throws Exception {
//		String streamName = testName.getMethodName() + new Random().nextInt();
//		String fileName = streamName + ".txt";
//		File file = new File(fileName);
//		try {
//			final HttpSource source = new HttpSource(shell);
//			final String stream = String.format("%s | file-logger --path=%s --inputType=text/plain", source, fileName);
//			createStream(streamName, stream);
//			source.ensureReady().postData(TEST_MESSAGE);
//			assertThat(file, eventually(50, 100, fileContent(endsWith(TEST_MESSAGE + System.lineSeparator()))));
//		}
//		finally {
//			streamOps.destroyStream(streamName);
//			if (file.exists()) {
//				file.delete();
//			}
//		}
//	}
//
//	@Test
//	public void testSparkScalaFileLogger() throws Exception {
//		String streamName = testName.getMethodName()  + new Random().nextInt();
//		String fileName = streamName + ".txt";
//		File file = new File(fileName);
//		try {
//			final HttpSource source = new HttpSource(shell);
//			final String stream = String.format("%s | file-logger-scala --path=%s", source, fileName);
//			createStream(streamName, stream);
//			source.ensureReady().postData(TEST_MESSAGE);
//			assertThat(file, eventually(50, 100, fileContent(endsWith(TEST_MESSAGE + System.lineSeparator()))));
//		}
//		finally {
//			streamOps.destroyStream(streamName);
//			if (file.exists()) {
//				file.delete();
//			}
//		}
//	}
//
//	@Test
//	public void testSparkScalaFileLoggerWithInputType() throws Exception {
//		String streamName = testName.getMethodName()  + new Random().nextInt();
//		String fileName = streamName + ".txt";
//		File file = new File(fileName);
//		try {
//			final HttpSource source = new HttpSource(shell);
//			final String stream = String.format("%s | file-logger-scala --path=%s --inputType=text/plain", source, fileName);
//			createStream(streamName, stream);
//			source.ensureReady().postData(TEST_MESSAGE);
//			assertThat(file, eventually(50, 100, fileContent(endsWith(TEST_MESSAGE + System.lineSeparator()))));
//		}
//		finally {
//			streamOps.destroyStream(streamName);
//			if (file.exists()) {
//				file.delete();
//			}
//		}
//	}

}
