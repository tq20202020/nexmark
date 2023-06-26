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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.nexmark.flink.ConsumerThread;
import com.github.nexmark.flink.DataReporter;
import com.github.nexmark.flink.generator.GeneratorConfig;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;
import java.io.FileReader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
	
	// Since Flink is no longer being used, this attribute was removed
	//private transient ListState<Long> checkpointedState;

	public NexmarkSourceFunction(GeneratorConfig config, EventDeserializer<T> deserializer) {
		this.config = config;
		this.deserializer = deserializer;

		// Removing this attribute as it was dependent on Flink
		//this.resultType = resultType;
	}

	// This opens a new NexmarkGenerator object based on the entered configuration
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
				//String json = objectMapper.writeValueAsString(next);

				//System.out.println(json);
				//publishMessage(next);

				// Debugging to see JSON string
				//System.out.println(json);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		// Writing to the JSON file
		ctx.writeJson();

		// Initializes the Consumer Kafka
		consumerKafka();

		// Delaying time to give time for setup of Consumer Kafka
		TimeUnit time = TimeUnit.SECONDS;
		time.sleep(60);

		// Using the Producer Kafka side now
		sendKafka(ctx.jsonFormat());

		// Delaying time to give time for the records to be created and 
		time.sleep(60);
	}

	/** Note:
	 *  The work in the following work below this comment involving Kafka is largely based on the Microsoft tutorial for implementing Kafka with Event Hubs.
	 *  Link: https://github.com/MicrosoftDocs/azure-docs/blob/main/articles/event-hubs/event-hubs-java-get-started-send.md
	 */
	// Topic is Event as events: Person, Auction, Bid are being sent and received
    private final static String TOPIC = "Event";

	// Not using Flink, only have 1 thread
    private final static int NUM_THREADS = 1;

	// Producer side of Kafka
    private static void sendKafka(ArrayList<String> messages) throws Exception {
        //Create Kafka Producer
        final Producer<Long, String> producer = createProducer();

        Thread.sleep(5000);

        final ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);

		// Only have 1, but preserving this structure gives later flexibility should more be added
        for (int i = 0; i < NUM_THREADS; i++) {
            executorService.execute(new DataReporter(producer, TOPIC, messages));
		}
    }

	// Creates Producer for Kafka
    private static Producer<Long, String> createProducer() {
        try{
            Properties properties = new Properties();

			// Loading the properties configuration for producer
            properties.load(new FileReader("C:\\Users\\t-thodan\\OneDrive - Microsoft\\nexmarkdatagen\\nexmark-flink\\src\\producer.config"));
            properties.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            return new KafkaProducer<>(properties);
        } catch (Exception e){
            System.out.println("Failed to create producer with exception: " + e);
            System.exit(0);

			// Return statement is required
            return null;
        }
    }


	// Initializes the consumer end of Kafka
    public static void consumerKafka() throws Exception {

		// Uses the same number of threads as Producer
        final ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);

		// Again, preserving this structure to allow for greater flexibility in the future should the number of threads be increased
        for (int i = 0; i < NUM_THREADS; i++) {
            executorService.execute(new ConsumerThread(TOPIC));
        }
    }
}
