/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.nexmark.flink.source;

//Removing Flink dependencies
//import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.github.nexmark.flink.NexmarkConfiguration;
import com.github.nexmark.flink.generator.GeneratorConfig;
import com.github.nexmark.flink.model.Event;

import org.junit.Test;

public class NexmarkSourceFunctionITCase {

	/** This is currently used to run the data generator as a cluster is no longer used to control data generation.
	 * To run, click run this test
	 * Settings can be configured based on needs below.
	 * @throws Exception
	 */
	@Test
	public void testDataStream() throws Exception {
		// Creating nexmark configuration for the data stream
		NexmarkConfiguration nexmarkConfig = new NexmarkConfiguration();

		// Populating all of the nexmark configuration settings
		// Can be changed as deemed fit
		nexmarkConfig.auctionProportion = 20;
		nexmarkConfig.bidProportion = 46;
		nexmarkConfig.personProportion = 30;

		// Creating a generator configuration for the data stream 
		// Can also be changed as necessary
		GeneratorConfig generatorConfig = new GeneratorConfig(
			nexmarkConfig, 
			System.currentTimeMillis(), 
			1, 
			1000, 
			1
		);

		// Trying to create a Java stream to replace the StreamExecutionEnvironment
		System.out.println("Testing Stream");

		// Opening the NexmarkSourceFunction, which uses the event deserializer to get a toString() version of it
		NexmarkSourceFunction nexFunc = new NexmarkSourceFunction<>(generatorConfig, (EventDeserializer<String>) Event::toString);
		nexFunc.open(null);
		SourceContext context = new SourceContext<>();

		// Used to track the events and write to the JSON file - please see class for details on implementation
		nexFunc.run(context);
	}
}
