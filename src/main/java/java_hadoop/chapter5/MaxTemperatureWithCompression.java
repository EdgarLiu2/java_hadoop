package java_hadoop.chapter5;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java_hadoop.chapter2.MaxTemperature;
import java_hadoop.chapter2.MaxTemperatureMapper;
import java_hadoop.chapter2.MaxTemperatureReducer;

public class MaxTemperatureWithCompression {

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: MaxTemperatureWithCompression <input path> <output path>");
			System.exit(-1);
		}

		Job job = new Job();
		job.setJarByClass(MaxTemperature.class);
		job.setJobName("Max temperature");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(MaxTemperatureMapper.class);
		job.setReducerClass(MaxTemperatureReducer.class);
		job.setCombinerClass(MaxTemperatureReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		/*
		 *  compress the output of a MapReduce job in the job configuration
		 *  mapreduce.output.fileoutputformat.compress = true
		 *  mapreduce.output.fileoutputformat.compress.codec = org.apache.hadoop.io.compress.GzipCodec
		 *                                                mapreduce.output.fileoutputformat.compress.type = RECORD | BLOCK
		 */
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}

// bin/hadoop jar ~/workspace/java_hadoop/target/java_hadoop-0.0.1-SNAPSHOT.jar java_hadoop.chapter5.MaxTemperatureWithCompression input/ncdc/sample.txt output
