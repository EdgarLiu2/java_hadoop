package java_hadoop.chapter8;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * A MapReduce program for packaging a collection of small files as a single SequenceFile.
 * 
 * @author liuzhao
 *
 */
public class SmallFilesToSequenceFileConverter extends Configured implements Tool {
	
	static class SequenceFileMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {
		
		private Text filenameKey;

		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		protected void map(NullWritable key, BytesWritable value,
				Mapper<NullWritable, BytesWritable, Text, BytesWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(filenameKey, value);
		}

		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		protected void setup(Mapper<NullWritable, BytesWritable, Text, BytesWritable>.Context context)
				throws IOException, InterruptedException {
			InputSplit split = context.getInputSplit();
			FileSplit fileSplit = (FileSplit) split;
			Path path = fileSplit.getPath();
			filenameKey = new Text(path.toString());
		}
		
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = JobBuilder.parseInputAndOuput(this, getConf(), args);
		if (job == null) {
			return -1;
		}
		
		job.setInputFormatClass(WholeFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);
		
		job.setMapperClass(SequenceFileMapper.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new SmallFilesToSequenceFileConverter(), args);
		System.exit(exitCode);
	}
}

// bin/hadoop jar ~/workspace/java_hadoop/target/java_hadoop-0.0.1-SNAPSHOT.jar java_hadoop.chapter8.SmallFilesToSequenceFileConverter -conf conf/hadoop-localhost.xml -D mapreduce.job.reduces=2 test/input  /user/liuzhao/output1