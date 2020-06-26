/**
 * 
 */
package java_hadoop.chapter9;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java_hadoop.chapter2.MaxTemperatureReducer;
import java_hadoop.chapter6.NcdcRecordParser;
import java_hadoop.chapter8.JobBuilder;

/**
 * Example 9-1 Application to run the maximum temperature job, including counting missing and malformed fields and quality codes.
 * 
 * @author liuzhao
 *
 */
public class MaxTemperatureWithCounters extends Configured implements Tool {
	enum Temperature {
		MISSING,
		MALFORMED
	}
	
	static class MaxTemperatureMapperWithCounters extends Mapper<LongWritable, Text, Text, IntWritable> {
		private NcdcRecordParser parser = new NcdcRecordParser();
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
		
			parser.parse(value);
			if (parser.isMalformedTemperature()) {
				System.err.println("Ignoring possibly corrupt input: " + value);
				context.getCounter(Temperature.MALFORMED).increment(1);
			} else if (parser.isMissingTemperature()) {
				context.getCounter(Temperature.MISSING).increment(1);
			} else if (parser.isValidTemperature()) {
				int airTemperature = parser.getAirTemperature();
				context.write(new Text(parser.getYear()), new IntWritable(airTemperature));
			}
			
			// dynamic counter
			context.getCounter("TemperatureQuality", parser.getQuality()).increment(1);
		}
	}
	

	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] args) throws Exception {
		Job job = JobBuilder.parseInputAndOuput(this, getConf(), args);
		if (job == null) {
			return -1;
		}
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(MaxTemperatureMapperWithCounters.class);
		job.setCombinerClass(MaxTemperatureReducer.class);
		job.setReducerClass(MaxTemperatureReducer.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MaxTemperatureWithCounters(), args);
		System.exit(exitCode);
	}

}

//bin/hadoop jar ~/workspace/GitHub/java_hadoop/target/java_hadoop-0.0.1-SNAPSHOT.jar java_hadoop.chapter9.MaxTemperatureWithCounters -conf conf/hadoop-local.xml test/input/ncdc/micro test/output
//bin/hadoop jar ~/workspace/GitHub/java_hadoop/target/java_hadoop-0.0.1-SNAPSHOT.jar java_hadoop.chapter9.MaxTemperatureWithCounters -conf conf/hadoop-local.xml test/input/ncdc/010010-99999-2019.gz test/output3