/**
 * 
 */
package java_hadoop.chapter9;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java_hadoop.chapter8.JobBuilder;

/**
 * Example 9-2 Application to calculate he proportion of records with missing temperature fields
 * 
 * @author liuzhao
 *
 */
public class MissingTemperatureFields extends Configured implements Tool {

	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 1) {
			JobBuilder.printUsage(this, "<job ID>");
			return -1;
		}
		
		String jobId = args[0];
		Cluster cluster = new Cluster(getConf());
		Job job = cluster.getJob(JobID.forName(jobId));
		
		if (job == null) {
			System.err.printf("No job with ID %s found.\n", jobId);
			return -1;
		}
		if (!job.isComplete()) {
			System.err.printf("Job %s is not complete.\n", jobId);
			return -1;
		}
		
		Counters counters = job.getCounters();
		long missing = counters.findCounter(MaxTemperatureWithCounters.Temperature.MISSING).getValue();
		long total = counters.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();
		System.out.printf("Records with missing temperature fields: %.2f%%", 100.0 * missing / total);
		
		return 0;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MissingTemperatureFields(), args);
		System.exit(exitCode);

	}

}

//bin/hadoop jar ~/workspace/GitHub/java_hadoop/target/java_hadoop-0.0.1-SNAPSHOT.jar java_hadoop.chapter9.MissingTemperatureFields -conf conf/hadoop-local.xml job_local707158180_0001