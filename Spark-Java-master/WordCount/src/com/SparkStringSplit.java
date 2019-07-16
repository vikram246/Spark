/**
 * 
 */
package com;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author vikram
 *
 */
public class SparkStringSplit {

	@SuppressWarnings("resource")
	public static void wordCountJava8(String filename) {
		// Define a configuration to use to interact with Spark
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Work Count App");

		// Create a Java version of the Spark Context from the configuration
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Load the input data, which is a text file read from the command line
		JavaRDD<String> input = sc.textFile(filename);

		// Java 8 with lambdas: split the input string into words
		JavaRDD<String> words = input.flatMap(s -> Arrays.asList(s.split(" ")).iterator());

		List<String> p = words.collect();

		p.forEach(a -> {
			System.out.println(a);
		});

		// Java 8 with lambdas: split the input string into line
		JavaRDD<String> lines = input.flatMap(s -> Arrays.asList(s.split(" ")).iterator());

		List<String> line = lines.collect();

		line.forEach(a -> {
			System.out.println(a);
		});
	}

	public static void main(String[] args) {
		if (args.length == 0) {
			System.out.println("Usage: WordCount <file>");
			System.exit(0);
		}

		wordCountJava8(args[0]);
	}

}
