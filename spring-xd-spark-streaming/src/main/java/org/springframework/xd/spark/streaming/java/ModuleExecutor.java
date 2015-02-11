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

package org.springframework.xd.spark.streaming.java;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaDStreamLike;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;

import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.core.ResolvableType;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;
import org.springframework.xd.spark.streaming.SparkMessageSender;
import org.springframework.xd.spark.streaming.SparkStreamingModuleExecutor;

/**
 * Invokes the process method of a {@link org.springframework.xd.spark.streaming.java.Processor}
 * and handles the output DStream if present.
 *
 * @author Ilayaperumal Gopinathan
 * @author Mark Fisher
 * @since 1.1
 */
@SuppressWarnings({"unchecked", "rawtypes", "serial"})
public class ModuleExecutor implements SparkStreamingModuleExecutor<JavaReceiverInputDStream, Processor>, Serializable {

	private static SparkMessageSender messageSender;

	private static final String PROCESSOR_METHOD_NAME = "process";

	@SuppressWarnings("rawtypes")
	public void execute(JavaReceiverInputDStream input, Processor processor, final SparkMessageSender sender) {
		Method[] methods = processor.getClass().getDeclaredMethods();
		Class<?> parameterType = null;
		for (Method method : methods) {
			if (method.getName().contains(PROCESSOR_METHOD_NAME)) {
				ResolvableType resolvableType = ResolvableType.forMethodParameter(method, 0);
				parameterType = resolvableType.getGeneric(0).resolve();
			}
		}
		Assert.notNull(parameterType, "The Processor should have process method implemented");
		Assert.notNull(parameterType);
		System.out.println("********* parameter type ********"+ parameterType);
		JavaDStream<?> convertedInput = null;
		if (!parameterType.isAssignableFrom(Message.class)) {
			convertedInput = input.flatMap(new FlatMapFunction<Message, Object>() {

				@Override
				public Iterable<Object> call(Message x) {
					return Arrays.asList(x.getPayload());
				}
			});
		}
		System.out.println("********* converted type ********"+ convertedInput);
		JavaDStreamLike output = (convertedInput != null) ? processor.process(convertedInput) :
				processor.process(input);
		if (output != null) {
			output.foreachRDD(new Function<JavaRDDLike, Void>() {

				@Override
				public Void call(final JavaRDDLike rdd) {
					rdd.foreachPartition(new VoidFunction<Iterator<?>>() {

						@Override
						public void call(Iterator<?> results) throws Exception {
							if (results.hasNext()) {
								if (messageSender == null) {
									messageSender = sender;
								}
								try {
									messageSender.start();
								}
								catch (NoSuchBeanDefinitionException e) {
									// ignore for the first time.
								}
								finally {
									if (sender != null && !sender.isRunning()) {
										messageSender.stop();
										messageSender = sender;
										messageSender.start();
									}
								}
								while (results.hasNext()) {
									Object next = results.next();
									Message message = (next instanceof Message) ? (Message) next :
											MessageBuilder.withPayload(next).build();
									messageSender.send(message);
								}
							}
							sender.stop();
						}
					});
					return null;
				}
			});
		}
	}

}
