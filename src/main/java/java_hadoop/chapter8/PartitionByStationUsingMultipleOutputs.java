package java_hadoop.chapter8;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java_hadoop.chapter6.NcdcRecordParser;

/**
 * Partitioning whole dataset into files named by the station ID using MultipleOutputs
 * 
 * @author liuzhao
 *
 */
public class PartitionByStationUsingMultipleOutputs extends Configured implements Tool {
	
	static class StationMapper extends Mapper<LongWritable, Text, Text, Text> {
		private NcdcRecordParser parser = new NcdcRecordParser();

		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			parser.parse(value);
			context.write(new Text(parser.getStationId()), value);
		}
		
	}
	
	static class MultipleOutputsReducer extends Reducer<Text, Text, NullWritable, Text> {
		private MultipleOutputs<NullWritable, Text> multipleOutputs;
		private NcdcRecordParser parser = new NcdcRecordParser();

		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#cleanup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		@Override
		protected void cleanup(Reducer<Text, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			multipleOutputs.close();
		}

		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			for(Text value : values) {
				// key, value, base path (relative to the output directory)
				// multipleOutputs.write(NullWritable.get(), value, key.toString());
				
				
				// partition the data by station and year, so that each year's data is contained in a directory named by station_id/year/part-r-00000
				parser.parse(value);
				String basePath = String.format("%s/%s/part", parser.getStationId(), parser.getYear());
				multipleOutputs.write(NullWritable.get(), value, basePath);
			}
		}

		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		@Override
		protected void setup(Reducer<Text, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
		}
		
		
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = JobBuilder.parseInputAndOuput(this, getConf(), args);
		if (job == null) {
			return -1;
		}
		
		job.setMapperClass(StationMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setReducerClass(MultipleOutputsReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new PartitionByStationUsingMultipleOutputs(), args);
		System.exit(exitCode);
	}

}
