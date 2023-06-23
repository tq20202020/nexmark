package com.github.nexmark.flink.generator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Write data to be read as a side input.
 *
 * <p>Contains pairs of a number and its string representation to model lookups of some enrichment
 * data by id.
 *
 * <p>Generated data covers the range {@code [0, sideInputRowCount)} so lookup joins on any
 * desired id field can be modeled by looking up {@code id % sideInputRowCount}.
 */
public class SideInputGenerator {

	private static final Option PATH = new Option("p", "path", true,
		"absolute path of the side input file.");

	private static final Option ROW_COUNT = new Option("n", "num", true,
		"row count of side input.");

	// Populates the sideinput.txt file with its input
	// Unsure if column names want to be added to it down the line
	public void prepareSideInput(int sideInputRowCount, String path) throws IOException {
		List<String> result = new ArrayList<>();

		for (int i = 0; i < sideInputRowCount; i++) {
			result.add(i + "," + i);
		}

		FileUtils.writeLines(new File(path), "UTF-8", result);
	}

	public static void main(String[] args) throws IOException, ParseException {
		if (args == null || args.length == 0) {
			throw new RuntimeException("Usage: -n 1000 -p /path/to/side_input.txt");
		}
		
		Options options = getOptions();
		DefaultParser parser = new DefaultParser();
		CommandLine line = parser.parse(options, args, true);
		String path = line.getOptionValue(PATH.getOpt());
		int sideInputRowCount = Integer.parseInt(line.getOptionValue(ROW_COUNT.getOpt()));
		SideInputGenerator generator = new SideInputGenerator();
		generator.prepareSideInput(sideInputRowCount, path);
	}

	private static Options getOptions() {
		Options options = new Options();
		options.addOption(PATH);
		options.addOption(ROW_COUNT);
		return options;
	}
}
