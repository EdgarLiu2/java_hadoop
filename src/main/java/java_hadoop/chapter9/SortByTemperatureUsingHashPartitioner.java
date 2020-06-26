package java_hadoop.chapter9;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java_hadoop.chapter8.JobBuilder;


/**
 * Example 9-4 A MapReduce program for sorting a SequenceFile with IntWritable keys using the default HashPartitioner.
 * 
 * @author liuzhao
 *
 */
public class SortByTemperatureUsingHashPartitioner extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Job job = JobBuilder.parseInputAndOuput(this, getConf(), args);
		if (job == null) {
			return -1;
		}
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setCompressOutput(job, false);
//		SequenceFileOutputFormat.setCompressOutput(job, true);
//		SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
//		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new SortByTemperatureUsingHashPartitioner(), args);
		System.exit(exitCode);
	}

}

// This command produces 30 output files, each of which is sorted.
//bin/hadoop jar ~/workspace/GitHub/java_hadoop/target/java_hadoop-0.0.1-SNAPSHOT.jar java_hadoop.chapter9.SortByTemperatureUsingHashPartitioner -D mapreduce.job.reduces=30 -conf conf/hadoop-local.xml test/output test/output2