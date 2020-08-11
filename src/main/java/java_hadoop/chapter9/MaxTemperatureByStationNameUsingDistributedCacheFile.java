package java_hadoop.chapter9;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java_hadoop.chapter6.NcdcRecordParser;
import java_hadoop.chapter8.JobBuilder;
import java_hadoop.chapter9.MaxTemperatureUsingSecondarySort.MaxTemperatureReducer;

/**
 * Example 9-13. Application to find the maximum temperature by station, showing station names from a lookup table passed as a distributed cache file
 * 
 * @author liuzhao
 *
 */
public class MaxTemperatureByStationNameUsingDistributedCacheFile extends Configured implements Tool {
	
	static class StationTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private NcdcRecordParser parser = new NcdcRecordParser();
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			parser.parse(value);
			if (parser.isValidTemperature()) {
				context.write(new Text(parser.getStationId()), new IntWritable(parser.getAirTemperature()));
			}
		}
		
	}
	
	static class MaxTemperatureReducerWithStationLookup extends Reducer<Text, IntWritable, Text, IntWritable> {
		private NcdcStationMetadata metadata;

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			String stationName = metadata.getStationName(key.toString());
			
			int maxValue = Integer.MIN_VALUE;
			for (IntWritable value : values) {
				maxValue = Math.max(maxValue, value.get());
			}
			context.write(new Text(stationName), new IntWritable(maxValue));
		}

		@Override
		protected void setup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			metadata = new NcdcStationMetadata();
			metadata.initialize(new File("stations-fixed-width.txt"));
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = JobBuilder.parseInputAndOuput(this, getConf(), args);
		if (job == null) {
			return -1;
		}
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(StationTemperatureMapper.class);
		job.setCombinerClass(MaxTemperatureReducer.class);
		job.setReducerClass(MaxTemperatureReducerWithStationLookup.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MaxTemperatureByStationNameUsingDistributedCacheFile(), args);
		System.exit(exitCode);

	}

}

class NcdcStationMetadata {

	public void initialize(File file) {
		// TODO Auto-generated method stub
		
	}

	public String getStationName(String key) {
		// TODO Auto-generated method stub
		return null;
	}
	
}

//bin/hadoop jar ~/workspace/GitHub/java_hadoop/target/java_hadoop-0.0.1-SNAPSHOT.jar java_hadoop.chapter9.MaxTemperatureByStationNameUsingDistributedCacheFile -conf conf/hadoop-local.xml -files test/input/ncdc/metadata/stations-fixed-width.txt test/input/ncdc/all test/output