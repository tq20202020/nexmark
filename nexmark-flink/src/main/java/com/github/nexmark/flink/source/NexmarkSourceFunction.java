package com.github.nexmark.flink.source;

//Removing Flink dependencies
//import org.apache.flink.api.common.state.ListState;
//import org.apache.flink.api.common.state.ListStateDescriptor;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.api.common.typeutils.base.LongSerializer;
//import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.runtime.state.FunctionInitializationContext;
//import org.apache.flink.runtime.state.FunctionSnapshotContext;
//import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
//import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
//import org.apache.flink.util.Preconditions;

import com.github.nexmark.flink.generator.NexmarkGenerator;
import com.github.nexmark.flink.model.Bid;
import com.github.nexmark.flink.model.Event;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ser.std.NumberSerializers.LongSerializer;
import com.github.nexmark.flink.generator.GeneratorConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import com.github.nexmark.flink.source.SourceContext;

import javax.security.auth.login.Configuration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

/** Have to re-write entire NexmarkSourceFunction.java
 * Entirely depended on Flink with its setup, data types, etc
 */
public class NexmarkSourceFunction<T> {

	/** Configuration for generator to use when reading synthetic events. May be split. */
	private final GeneratorConfig config;

	private final EventDeserializer<T> deserializer;

	//private final TypeInformation<T> resultType;

	private transient NexmarkGenerator generator;

	/** The number of elements emitted already. */
	private volatile long numElementsEmitted;

	/** Flag to make the source cancelable. */
	private volatile boolean isRunning = true;
	
	//private transient ListState<Long> checkpointedState;

	public NexmarkSourceFunction(GeneratorConfig config, EventDeserializer<T> deserializer) {
		this.config = config;
		this.deserializer = deserializer;

		// Removing this attribute as it was dependent on Flink
		//this.resultType = resultType;
	}

	public void open(Map<String, Object> parameters) throws Exception {
		this.generator = new NexmarkGenerator(this.config);
	}

	/** This method is intended to replace the functionality of the .execute() method used in Flink. */
	public void run(SourceContext<T> ctx) throws Exception {
		ObjectMapper objectMapper = new ObjectMapper();

		// White status isRunning is true and there are more events
		while (isRunning && generator.hasNext()) {
			long now = System.currentTimeMillis();
			NexmarkGenerator.NextEvent nextEvent = generator.nextEvent();

			if (nextEvent.wallclockTimestamp > now) {
				// sleep until wall clock less than current timestamp.
				TimeUnit.SECONDS.sleep(nextEvent.wallclockTimestamp - now);
				//Thread.sleep(nextEvent.wallclockTimestamp - now);
			}

			// Deserializing the next event
			T next = deserializer.deserialize(nextEvent.event);
			// Tracking the number of events so far
			numElementsEmitted = generator.getEventsCountSoFar();

			try {
				// Stores all of the created events - see SourceContext.java for method implementation
				ctx.collect(next);

				// Debugging Information (Prints the Event Instances)
				//System.out.println(next);

				// JSON Formatting
				String json = objectMapper.writeValueAsString(next);
				publishMessage(json);

				// Debugging to see JSON string
				//System.out.println(json);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		// Writing to the JSON file
		ctx.writeJson();
	}

	// Autowiring Kafka Template
    @Autowired KafkaTemplate<String, String> kafkaTemplate;
	private static final String TOPIC = "NewEvent";

    // Annotation
    @PostMapping("/publish")
    public void publishMessage(@RequestBody String event) throws Exception {
        // Sending the message
        System.out.println(kafkaTemplate.send(TOPIC, event).toString());
    }
}
