package java_hadoop.chapter9;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java_hadoop.chapter8.JobBuilder;

/**
 * Example 9-5. A MapReduce program for sorting a SequenceFile with IntWritable keys using the TotalOrderPartitioner to globally sort the data.
 * 
 * @author liuzhao
 *
 */
public class SortByTemperatureUsingTotalOrderPartitioner extends Configured implements Tool {

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
		
		job.setPartitionerClass(TotalOrderPartitioner.class);
		
		/*
		 * We use a RandomSampler, which chooses keys with a uniform probability — here, 0.1. 
		 * There are also parameters for the maximum number of samples to take and the maximum number of splits to sample (here, 10,000 and 10, respectively;
		 * these settings are the defaults when InputSampler is run as an application),
		 * and the sampler stops when the first of these limits is met. 
		 */
		InputSampler.Sampler<IntWritable, Text> sampler = new InputSampler.RandomSampler<IntWritable, Text>(0.1, 10000, 10);
		InputSampler.writePartitionFile(job, sampler);
		
		// Add to DistributedCache
		Configuration conf = job.getConfiguration();
		String partitionFile = TotalOrderPartitioner.getPartitionFile(conf);
		URI partitionUri = new URI(partitionFile);
		job.addCacheFile(partitionUri);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new SortByTemperatureUsingTotalOrderPartitioner(), args);
		System.exit(exitCode);
	}

}


//bin/hadoop jar ~/workspace/GitHub/java_hadoop/target/java_hadoop-0.0.1-SNAPSHOT.jar java_hadoop.chapter9.SortByTemperatureUsingTotalOrderPartitioner -D mapreduce.job.reduces=2 -conf conf/hadoop-local.xml test/input/ncdc/Seq-file test/output