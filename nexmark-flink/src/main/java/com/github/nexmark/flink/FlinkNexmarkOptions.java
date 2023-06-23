package com.github.nexmark.flink;

//Removing Flink depenencies
//import org.apache.flink.configuration.ConfigOption;
//import org.apache.flink.configuration.ConfigOptions;

import java.time.Duration;

/**
 * Options to control the nexmark-flink benchmark behaviors.
 */
public class FlinkNexmarkOptions {

	/**
	public static final ConfigOption<Duration> METRIC_MONITOR_DELAY = ConfigOptions
		.key("nexmark.metric.monitor.delay")
		.durationType()
		.defaultValue(Duration.ofSeconds(10))
		.withDescription("When to monitor the metrics, default 10secs after job is started");

	public static final ConfigOption<Duration> METRIC_MONITOR_DURATION = ConfigOptions
		.key("nexmark.metric.monitor.duration")
		.durationType()
		.defaultValue(Duration.ofMillis(Long.MAX_VALUE))
		.withDescription("How long to monitor the metrics, default never end, " +
			"monitor until job is finished.");

	public static final ConfigOption<Duration> METRIC_MONITOR_INTERVAL = ConfigOptions
		.key("nexmark.metric.monitor.interval")
		.durationType()
		.defaultValue(Duration.ofSeconds(5))
		.withDescription("The interval to request the metrics.");


	public static final ConfigOption<String> METRIC_REPORTER_HOST = ConfigOptions
		.key("nexmark.metric.reporter.host")
		.stringType()
		.defaultValue("localhost")
		.withDescription("The metric reporter server host.");

	public static final ConfigOption<Integer> METRIC_REPORTER_PORT = ConfigOptions
		.key("nexmark.metric.reporter.port")
		.intType()
		.defaultValue(9098)
		.withDescription("The metric reporter server port.");

	public static final ConfigOption<String> FLINK_REST_ADDRESS = ConfigOptions
		.key("flink.rest.address")
		.stringType()
		.defaultValue("localhost")
		.withDescription("Flink REST address.");

	public static final ConfigOption<Integer> FLINK_REST_PORT = ConfigOptions
		.key("flink.rest.port")
		.intType()
		.defaultValue(8081)
		.withDescription("Flink REST port.");
	*/
}
