package java_hadoop.chapter9;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java_hadoop.chapter5.TextPair;
import java_hadoop.chapter6.NcdcRecordParser;
import java_hadoop.chapter8.JobBuilder;

/**
 * Example 9-12. Application to join weather records with station names
 * 
 * @author liuzhao
 *
 */
public class JoinRecordWithStationName extends Configured implements Tool {
	
	public static class KeyPartitioner extends Partitioner<TextPair, Text> {
		@Override
		public int getPartition(TextPair key, Text value, int numPartitions) {
			return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
		
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			JobBuilder.printUsage(this, "<ncdc input> <station input> <output>");
			return -1;
		}
		
		Job job = new Job(getConf(), "Join weather records with station names");
		job.setJarByClass(getClass());
		
		Path ncdcInputPath = new Path(args[0]);
		Path stationInputPath = new Path(args[1]);
		Path outputPath = new Path(args[2]);
		
		MultipleInputs.addInputPath(job, ncdcInputPath, TextInputFormat.class, JoinRecordMapper.class);
		MultipleInputs.addInputPath(job, stationInputPath, TextInputFormat.class, JoinStationMapper.class);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.setPartitionerClass(KeyPartitioner.class);
//		job.setGroupingComparatorClass(TextPair.FirstComparator.class);
		job.setMapOutputKeyClass(TextPair.class);
		job.setReducerClass(JoinReducer.class);
		job.setOutputKeyClass(Text.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new JoinRecordWithStationName(), args);
		System.exit(exitCode);
	}
}


class NcdcStationMetadataParser {
	public boolean parse(Text value) {
		return true;
	}
	
	public String getStationId() {
		return "";
	}
	
	public String getStationName() {
		return "";
	}
}

/**
 * Example 9-9. Mapper for tagging station records for a reduce-side join
 * 
 * @author liuzhao
 *
 */
class JoinStationMapper extends Mapper<LongWritable, Text, TextPair, Text> {
	
	private NcdcStationMetadataParser parser = new NcdcStationMetadataParser();

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, TextPair, Text>.Context context)
			throws IOException, InterruptedException {
		if (parser.parse(value)) {
			context.write(new TextPair(parser.getStationId(),  "0"), new Text(parser.getStationName()));
		}
	}
	
}

/**
 * Example 9-10 Mapper for tagging weather records for a reduce-side join
 * 
 * @author liuzhao
 *
 */
class JoinRecordMapper extends Mapper<LongWritable, Text, TextPair, Text> {
	private NcdcRecordParser parser = new NcdcRecordParser();

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, TextPair, Text>.Context context)
			throws IOException, InterruptedException {
		parser.parse(value);
		context.write(new TextPair(parser.getStationId(), "1"), value);
	}
	
}

/**
 * Example 9-11 Reducer for joining tagged station records with tagged weather records
 * 
 * @author liuzhao
 *
 */
class JoinReducer extends Reducer<TextPair, Text, Text, Text> {

	@Override
	protected void reduce(TextPair key, Iterable<Text> values, Reducer<TextPair, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		Iterator<Text> iter = values.iterator();
		Text stationName = new Text(iter.next());
		while (iter.hasNext()) {
			Text record = iter.next();
			Text outValue = new Text(stationName.toString() + "\t" + record.toString());
			context.write(key.getFirst(), outValue);
		}
	}
	
}